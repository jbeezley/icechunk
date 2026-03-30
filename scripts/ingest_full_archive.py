#!/usr/bin/env python3
"""Ingest the full GRIB 0p125 archive (00z/12z) into an icechunk store.

Usage (S3 store):
    python scripts/ingest_full_archive.py \
        --bucket my-icechunk-bucket --prefix grib-archive \
        --workers 32 --batch-size 50

Usage (local store, for testing):
    python scripts/ingest_full_archive.py \
        --local /tmp/grib_full_store \
        --workers 4 --batch-size 5 \
        --start-date 20260101 --end-date 20260110

Requires AWS credentials configured for both the index bucket
(spire-wx-products-metadata) and the store bucket.
"""

import argparse
import gzip
import json
import logging
import multiprocessing as mp
import os
import pickle
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
import numpy as np
import yaml
import zarr

from icechunk import Repository, VirtualChunkSpec, local_filesystem_storage, s3_storage
from scripts.grib_index import GribCatalog, GribMessage

log = logging.getLogger("ingest")

INDEX_BUCKET = "spire-wx-products-metadata"
INDEX_PREFIX = "sidecar/spire-wx-products-archive/mpp"


# ── Phase 1: Enumerate cycles ────────────────────────────────────────


def enumerate_cycles(
    start: date, end: date, hours: tuple[str, ...] = ("00", "12")
) -> list[tuple[str, str]]:
    """Return (date_str, hour_str) for every date in range.

    0p125 data exists for every date from ~2019-08-01 onward.
    Workers handle missing cycles gracefully.
    """
    cycles = []
    d = start
    while d <= end:
        ds = d.strftime("%Y%m%d")
        for hour in hours:
            cycles.append((ds, hour))
        d += timedelta(days=1)
    return cycles


# ── Phase 2: Variable catalog ────────────────────────────────────────


@dataclass
class VariableSpec:
    """Specification for one zarr array."""

    name: str
    levels: list[float]  # sorted; empty for single-level
    ni: int = 2880
    nj: int = 1441


def _parse_index_bytes(raw_gz: bytes) -> tuple[str, list[dict]]:
    """Parse a gzipped yaml index, return (grib_uri, messages)."""
    text = gzip.decompress(raw_gz).decode("utf-8")
    data = yaml.safe_load(text)
    return data["uri"], data["messages"]


def build_catalog(
    representative_dates: list[str],
) -> dict[str, VariableSpec]:
    """Scan f000 from representative cycles to build the union catalog."""
    s3 = boto3.client("s3")
    catalog: dict[str, VariableSpec] = {}

    for ds in representative_dates:
        key = f"{INDEX_PREFIX}/{ds}/00/upp.t00z.pgrb2.0p125.f000.yaml.gz"
        try:
            resp = s3.get_object(Bucket=INDEX_BUCKET, Key=key)
        except Exception:
            log.warning("Skipping %s (not found)", ds)
            continue

        _, messages = _parse_index_bytes(resp["Body"].read())
        for msg in messages:
            computed = msg["computed"]
            section3 = msg["sections"][3]
            name = f"{computed['shortName']}_{computed['typeOfLevel']}"
            level = computed["level"]

            if name not in catalog:
                catalog[name] = VariableSpec(
                    name=name,
                    levels=[],
                    ni=section3["Ni"],
                    nj=section3["Nj"],
                )
            spec = catalog[name]
            if level not in spec.levels:
                spec.levels.append(level)

    # Sort levels
    for spec in catalog.values():
        spec.levels.sort()

    return catalog


def get_forecast_hours(date_str: str = "20260101", hour: str = "00") -> list[int]:
    """Get the superset of forecast hours from a recent cycle."""
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    fh_set: set[int] = set()
    for page in paginator.paginate(
        Bucket=INDEX_BUCKET, Prefix=f"{INDEX_PREFIX}/{date_str}/{hour}/"
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"].split("/")[-1]
            if "0p125" in key:
                m = re.search(r"\.f(\d+)\.", key)
                if m:
                    fh_set.add(int(m.group(1)))
    return sorted(fh_set)


# ── Phase 3: Store creation ──────────────────────────────────────────


def create_store_structure(
    repo: Repository,
    catalog: dict[str, VariableSpec],
    time_values: np.ndarray,
    forecast_hours: list[int],
) -> None:
    """Create all zarr arrays and coordinate arrays in the store."""
    session = repo.writable_session("main")
    store = session.store
    root = zarr.open_group(store, mode="w")

    n_times = len(time_values)
    n_fh = len(forecast_hours)

    # Track unique level dimensions
    level_dims: dict[str, list[float]] = {}

    for spec in sorted(catalog.values(), key=lambda s: s.name):
        multi_level = len(spec.levels) > 1

        if multi_level:
            level_dim = f"level_{len(spec.levels)}"
            level_dims[level_dim] = [float(lv) for lv in spec.levels]
            shape = (n_times, n_fh, len(spec.levels), spec.nj, spec.ni)
            chunks = (1, 1, 1, spec.nj, spec.ni)
            dim_names = ("time", "forecast_hour", level_dim, "latitude", "longitude")
        else:
            shape = (n_times, n_fh, spec.nj, spec.ni)
            chunks = (1, 1, spec.nj, spec.ni)
            dim_names = ("time", "forecast_hour", "latitude", "longitude")

        root.create_array(
            spec.name,
            shape=shape,
            chunks=chunks,
            dtype="float32",
            dimension_names=dim_names,
            fill_value=np.nan,
        )

    # ── Coordinate arrays ──
    # Time
    time_arr = root.create_array(
        "time",
        shape=(n_times,),
        chunks=(n_times,),
        dtype="int64",
        dimension_names=("time",),
    )
    time_arr[:] = time_values.astype("datetime64[ns]").astype("int64")
    time_arr.attrs["units"] = "nanoseconds since 1970-01-01"
    time_arr.attrs["calendar"] = "proleptic_gregorian"

    # Forecast hour
    fh_arr = root.create_array(
        "forecast_hour",
        shape=(n_fh,),
        chunks=(n_fh,),
        dtype="int32",
        dimension_names=("forecast_hour",),
    )
    fh_arr[:] = np.array(forecast_hours, dtype="int32")

    # Level coordinate arrays
    for level_dim, levels in level_dims.items():
        lv_arr = root.create_array(
            level_dim,
            shape=(len(levels),),
            chunks=(len(levels),),
            dtype="float64",
            dimension_names=(level_dim,),
        )
        lv_arr[:] = np.array(levels, dtype="float64")

    session.commit("create store structure")
    log.info(
        "Created store: %d arrays, %d times, %d forecast hours",
        len(catalog),
        n_times,
        n_fh,
    )


# ── Phase 4: Worker function ─────────────────────────────────────────


def _download_one(s3_client, key: str) -> bytes:
    resp = s3_client.get_object(Bucket=INDEX_BUCKET, Key=key)
    return resp["Body"].read()


def ingest_cycles_worker(
    fork_bytes: bytes,
    cycles: list[tuple[str, str]],
    time_index: dict[str, int],
    fh_index: dict[int, int],
    catalog_levels: dict[str, list[float]],
    download_threads: int = 8,
) -> bytes:
    """Worker: download, parse, write virtual refs for assigned cycles.

    Returns the modified fork session as bytes.
    """
    fork = pickle.loads(fork_bytes)
    store = fork.store
    s3 = boto3.client("s3")

    for date_str, hour in cycles:
        time_key = f"{date_str}/{hour}"
        ti = time_index[time_key]

        # List yaml.gz files for this cycle
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(
            Bucket=INDEX_BUCKET,
            Prefix=f"{INDEX_PREFIX}/{date_str}/{hour}/",
        ):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".yaml.gz") and "0p125" in obj["Key"]:
                    keys.append(obj["Key"])

        # Download in parallel using threads
        raw_data: dict[str, bytes] = {}
        with ThreadPoolExecutor(max_workers=download_threads) as tpool:
            futures = {
                tpool.submit(_download_one, s3, key): key for key in keys
            }
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    raw_data[key] = fut.result()
                except Exception as e:
                    sys.stderr.write(f"WARN: download failed {key}: {e}\n")

        # Parse and build virtual refs per array
        array_specs: dict[str, list[VirtualChunkSpec]] = {}

        for key in sorted(raw_data):
            try:
                grib_uri, messages = _parse_index_bytes(raw_data[key])
            except Exception as e:
                sys.stderr.write(f"WARN: parse failed {key}: {e}\n")
                continue

            for msg in messages:
                computed = msg["computed"]
                section3 = msg["sections"][3]
                name = f"{computed['shortName']}_{computed['typeOfLevel']}"
                level = computed["level"]
                step_str = computed["stepRange"]
                fh = int(step_str.split("-")[-1])

                if fh not in fh_index:
                    continue
                fi = fh_index[fh]

                levels = catalog_levels.get(name)
                if levels is None:
                    # Variable not in catalog (unexpected)
                    continue

                if len(levels) > 1:
                    # Multi-level
                    if level not in levels:
                        continue
                    li = levels.index(level)
                    index = [ti, fi, li, 0, 0]
                else:
                    index = [ti, fi, 0, 0]

                array_specs.setdefault(name, []).append(
                    VirtualChunkSpec(
                        index=index,
                        location=grib_uri,
                        offset=msg["offset"],
                        length=msg["length"],
                    )
                )

        # Write virtual refs
        for array_name, vspecs in array_specs.items():
            store.set_virtual_refs(
                array_path=f"/{array_name}",
                validate_containers=False,
                chunks=vspecs,
            )

    return pickle.dumps(fork)


# ── Phase 5: Orchestration ───────────────────────────────────────────


def ingest_batch(
    repo: Repository,
    batch_cycles: list[tuple[str, str]],
    time_index: dict[str, int],
    fh_index: dict[int, int],
    catalog_levels: dict[str, list[float]],
    n_workers: int,
    download_threads: int,
) -> None:
    """Ingest a batch of cycles using ProcessPoolExecutor + fork/merge."""
    session = repo.writable_session("main")

    # Split cycles across workers
    chunk_size = max(1, len(batch_cycles) // n_workers)
    worker_chunks = []
    for i in range(0, len(batch_cycles), chunk_size):
        worker_chunks.append(batch_cycles[i : i + chunk_size])

    # Fork once and serialize
    fork = session.fork()
    fork_bytes = pickle.dumps(fork)

    results = []
    with ProcessPoolExecutor(
        max_workers=min(n_workers, len(worker_chunks)),
        mp_context=mp.get_context("spawn"),
    ) as pool:
        futures = {
            pool.submit(
                ingest_cycles_worker,
                fork_bytes,
                chunk,
                time_index,
                fh_index,
                catalog_levels,
                download_threads,
            ): i
            for i, chunk in enumerate(worker_chunks)
        }
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                result_bytes = fut.result()
                results.append(result_bytes)
            except Exception as e:
                log.error("Worker %d failed: %s", idx, e)

    # Merge all forks
    forks = [pickle.loads(b) for b in results]
    if forks:
        session.merge(*forks)
        session.commit(
            f"ingest {len(batch_cycles)} cycles: "
            f"{batch_cycles[0][0]} - {batch_cycles[-1][0]}"
        )


def save_progress(path: str, completed_batches: int, total_batches: int):
    with open(path, "w") as f:
        json.dump(
            {"completed_batches": completed_batches, "total_batches": total_batches},
            f,
        )


def load_progress(path: str) -> int:
    try:
        with open(path) as f:
            return json.load(f)["completed_batches"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Ingest the full GRIB 0p125 archive into icechunk"
    )

    storage_group = parser.add_mutually_exclusive_group(required=True)
    storage_group.add_argument("--local", help="Local filesystem path for store")
    storage_group.add_argument("--bucket", help="S3 bucket for store")
    parser.add_argument("--prefix", help="S3 prefix for store", default="grib-archive")
    parser.add_argument("--region", help="S3 region", default=None)

    parser.add_argument("--workers", type=int, default=32, help="Number of worker processes")
    parser.add_argument("--download-threads", type=int, default=8, help="Download threads per worker")
    parser.add_argument("--batch-size", type=int, default=50, help="Cycles per commit batch")

    parser.add_argument("--start-date", default="20190801", help="Start date YYYYMMDD")
    parser.add_argument("--end-date", default="20260330", help="End date YYYYMMDD")

    parser.add_argument("--progress-file", default="ingest_progress.json", help="Progress tracking file")
    parser.add_argument("--catalog-file", default="variable_catalog.json", help="Cached variable catalog")
    parser.add_argument("--dry-run", action="store_true", help="Just print plan, don't ingest")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    start_d = date(int(args.start_date[:4]), int(args.start_date[4:6]), int(args.start_date[6:]))
    end_d = date(int(args.end_date[:4]), int(args.end_date[4:6]), int(args.end_date[6:]))

    # ── Phase 1: Enumerate cycles ──
    log.info("Enumerating cycles from %s to %s ...", start_d, end_d)
    cycles = enumerate_cycles(start_d, end_d)
    log.info("Found %d cycles", len(cycles))

    if not cycles:
        log.error("No cycles found")
        return

    # Build time values and index
    time_values = []
    time_index: dict[str, int] = {}
    for i, (ds, hr) in enumerate(cycles):
        dt = datetime(int(ds[:4]), int(ds[4:6]), int(ds[6:]), int(hr), tzinfo=timezone.utc)
        time_values.append(np.datetime64(dt.replace(tzinfo=None), "ns"))
        time_index[f"{ds}/{hr}"] = i
    time_values = np.array(time_values)

    # ── Phase 2: Variable catalog ──
    if os.path.exists(args.catalog_file):
        log.info("Loading cached catalog from %s", args.catalog_file)
        with open(args.catalog_file) as f:
            raw = json.load(f)
        catalog = {
            name: VariableSpec(name=name, levels=spec["levels"])
            for name, spec in raw.items()
        }
    else:
        log.info("Building variable catalog from representative cycles...")
        rep_dates = [
            "20190801", "20200101", "20210101", "20220101",
            "20230101", "20240101", "20250101", "20260101",
        ]
        catalog = build_catalog(rep_dates)
        # Cache it
        raw = {name: {"levels": spec.levels} for name, spec in catalog.items()}
        with open(args.catalog_file, "w") as f:
            json.dump(raw, f, indent=2)
        log.info("Saved catalog (%d variables) to %s", len(catalog), args.catalog_file)

    # ── Forecast hours ──
    forecast_hours = get_forecast_hours()
    fh_index = {fh: i for i, fh in enumerate(forecast_hours)}

    # Catalog levels lookup for workers (avoid sending full VariableSpec)
    catalog_levels = {
        name: spec.levels for name, spec in catalog.items()
    }

    # ── Summary ──
    n_batches = (len(cycles) + args.batch_size - 1) // args.batch_size
    log.info(
        "Plan: %d cycles, %d variables, %d forecast hours, "
        "%d batches of %d, %d workers",
        len(cycles),
        len(catalog),
        len(forecast_hours),
        n_batches,
        args.batch_size,
        args.workers,
    )

    if args.dry_run:
        log.info("Dry run — exiting.")
        return

    # ── Phase 3: Create or open store ──
    if args.local:
        storage = local_filesystem_storage(args.local)
    else:
        storage = s3_storage(
            bucket=args.bucket,
            prefix=args.prefix,
            region=args.region,
            from_env=True,
        )

    completed_batches = load_progress(args.progress_file)

    if completed_batches == 0:
        log.info("Creating store...")
        repo = Repository.create(storage)
        create_store_structure(repo, catalog, time_values, forecast_hours)
    else:
        log.info("Resuming from batch %d/%d", completed_batches, n_batches)
        repo = Repository.open(storage)

    # ── Phase 4: Ingest in batches ──
    all_batches = []
    for i in range(0, len(cycles), args.batch_size):
        all_batches.append(cycles[i : i + args.batch_size])

    t_start = time.perf_counter()

    for batch_idx, batch_cycles in enumerate(all_batches):
        if batch_idx < completed_batches:
            continue

        t_batch = time.perf_counter()
        log.info(
            "Batch %d/%d: %d cycles (%s - %s)",
            batch_idx + 1,
            n_batches,
            len(batch_cycles),
            batch_cycles[0][0],
            batch_cycles[-1][0],
        )

        ingest_batch(
            repo,
            batch_cycles,
            time_index,
            fh_index,
            catalog_levels,
            args.workers,
            args.download_threads,
        )

        elapsed_batch = time.perf_counter() - t_batch
        elapsed_total = time.perf_counter() - t_start
        remaining = (n_batches - batch_idx - 1) * elapsed_batch
        log.info(
            "  Batch done in %.1fs. Total: %.1fs. Est remaining: %.0fs (%.1fh)",
            elapsed_batch,
            elapsed_total,
            remaining,
            remaining / 3600,
        )

        save_progress(args.progress_file, batch_idx + 1, n_batches)

    elapsed = time.perf_counter() - t_start
    log.info("Done! %d cycles ingested in %.1fs (%.1fh)", len(cycles), elapsed, elapsed / 3600)


if __name__ == "__main__":
    main()
