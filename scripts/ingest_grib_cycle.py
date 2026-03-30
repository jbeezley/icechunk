"""Ingest a single GRIB forecast cycle into an icechunk store.

Usage:
    python scripts/ingest_grib_cycle.py \
        --date 20260101 --hour 00 \
        --output /tmp/grib_store
"""

import argparse
import gzip
from pathlib import Path

import boto3
import numpy as np
import yaml
import zarr

from icechunk import (
    Repository,
    VirtualChunkSpec,
    local_filesystem_storage,
)
from scripts.grib_index import GribCatalog, GribMessage


S3_INDEX_BUCKET = "spire-wx-products-metadata"
S3_INDEX_PREFIX = "sidecar/spire-wx-products-archive/mpp"


def list_index_files(date: str, hour: str) -> list[str]:
    """List all yaml.gz index files for a cycle."""
    s3 = boto3.client("s3")
    prefix = f"{S3_INDEX_PREFIX}/{date}/{hour}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=S3_INDEX_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".yaml.gz"):
                keys.append(obj["Key"])
    return sorted(keys)


def download_and_parse(keys: list[str]) -> GribCatalog:
    """Download yaml.gz files from S3 and parse into a catalog."""
    s3 = boto3.client("s3")
    catalog = GribCatalog()

    for i, key in enumerate(keys):
        print(f"  Parsing {i + 1}/{len(keys)}: {key.split('/')[-1]}")
        resp = s3.get_object(Bucket=S3_INDEX_BUCKET, Key=key)
        raw = resp["Body"].read()
        text = gzip.decompress(raw).decode("utf-8")
        data = yaml.safe_load(text)

        grib_uri = data["uri"]
        for msg in data["messages"]:
            computed = msg["computed"]
            section3 = msg["sections"][3]
            step_str = computed["stepRange"]
            forecast_hour = int(step_str.split("-")[-1])

            catalog.messages.append(
                GribMessage(
                    short_name=computed["shortName"],
                    type_of_level=computed["typeOfLevel"],
                    level=computed["level"],
                    forecast_hour=forecast_hour,
                    uri=grib_uri,
                    offset=msg["offset"],
                    length=msg["length"],
                    ni=section3["Ni"],
                    nj=section3["Nj"],
                )
            )

    return catalog


def create_store(catalog: GribCatalog, output_path: str) -> None:
    """Create icechunk store with zarr arrays and virtual refs."""
    repo = Repository.create(
        local_filesystem_storage(output_path),
    )
    session = repo.writable_session("main")
    store = session.store

    groups = catalog.groups()
    print(f"\nCreating {len(groups)} arrays from {len(catalog.messages)} messages")

    root = zarr.open_group(store, mode="w")

    # Track unique level sets for coordinate arrays
    level_sets: dict[str, list[int]] = {}

    for array_name, messages in sorted(groups.items()):
        # Determine dimensions
        forecast_hours = sorted(set(m.forecast_hour for m in messages))
        levels = sorted(set(m.level for m in messages))
        ni = messages[0].ni
        nj = messages[0].nj
        multi_level = len(levels) > 1

        fh_index = {fh: i for i, fh in enumerate(forecast_hours)}
        lv_index = {lv: i for i, lv in enumerate(levels)}

        if multi_level:
            level_dim = f"level_{len(levels)}"
            level_sets[level_dim] = levels
            shape = (len(forecast_hours), len(levels), nj, ni)
            chunks = (1, 1, nj, ni)
            dim_names = ("forecast_hour", level_dim, "latitude", "longitude")
        else:
            shape = (len(forecast_hours), nj, ni)
            chunks = (1, nj, ni)
            dim_names = ("forecast_hour", "latitude", "longitude")

        root.create_array(
            array_name,
            shape=shape,
            chunks=chunks,
            dtype="float32",
            dimension_names=dim_names,
            fill_value=np.nan,
        )

        # Build virtual chunk specs
        vspecs = []
        for msg in messages:
            fi = fh_index[msg.forecast_hour]
            if multi_level:
                li = lv_index[msg.level]
                index = [fi, li, 0, 0]
            else:
                index = [fi, 0, 0]

            vspecs.append(VirtualChunkSpec(
                index=index,
                location=msg.uri,
                offset=msg.offset,
                length=msg.length,
            ))

        store.set_virtual_refs(
            array_path=f"/{array_name}",
            validate_containers=False,
            chunks=vspecs,
        )

        print(f"  {array_name}: shape={shape} chunks={len(vspecs)}")

    # Create coordinate arrays as materialized data
    all_fh = sorted(set(m.forecast_hour for m in catalog.messages))

    fh_arr = root.create_array(
        "forecast_hour",
        shape=(len(all_fh),),
        chunks=(len(all_fh),),
        dtype="int32",
        dimension_names=("forecast_hour",),
    )
    fh_arr[:] = np.array(all_fh, dtype="int32")

    # Create level coordinate arrays for each unique level dimension
    for level_dim, levels in level_sets.items():
        lv_arr = root.create_array(
            level_dim,
            shape=(len(levels),),
            chunks=(len(levels),),
            dtype="float64",
            dimension_names=(level_dim,),
        )
        lv_arr[:] = np.array(levels, dtype="float64")

    session.commit("ingest GRIB cycle")
    print(f"\nCommitted. Store at: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest a GRIB forecast cycle into icechunk"
    )
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--hour", required=True, help="HH (e.g., 00)")
    parser.add_argument("--output", required=True, help="Local path for store")
    args = parser.parse_args()

    print(f"Listing index files for {args.date}/{args.hour}...")
    keys = list_index_files(args.date, args.hour)
    print(f"Found {len(keys)} index files")

    print("Downloading and parsing...")
    catalog = download_and_parse(keys)
    print(f"Parsed {len(catalog.messages)} messages")

    create_store(catalog, args.output)


if __name__ == "__main__":
    main()
