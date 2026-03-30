"""Test get_object_references() queries against the GRIB icechunk store.

Must be run with the xarray from ~/git/xarray:
    PYTHONPATH=~/git/xarray python scripts/test_grib_queries.py \
        --store /tmp/grib_mvp_store
"""

import argparse
import sys
import time

import numpy as np

# Ensure we use the custom xarray
sys.path.insert(0, str(__import__("pathlib").Path.home() / "git" / "xarray"))

import xarray as xr

from icechunk import Repository, local_filesystem_storage


def timed(label, fn):
    """Run fn, print timing, return result."""
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    print(f"\n{'=' * 60}")
    print(f"Query: {label}")
    print(f"Time:  {elapsed:.4f}s")
    return result, elapsed


def print_refs(refs: dict):
    """Print summary of object references."""
    total = sum(len(v) for v in refs.values())
    print(f"Variables with refs: {len(refs)}")
    print(f"Total refs: {total}")
    for var_name, var_refs in sorted(refs.items()):
        if var_refs:
            print(f"  {var_name}: {len(var_refs)} refs")
            # Show first ref as sample
            r = var_refs[0]
            print(f"    sample: {r.uri}  offset={r.byte_offset}  length={r.byte_length}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", required=True)
    args = parser.parse_args()

    print(f"Opening store at {args.store}")
    repo = Repository.open(local_filesystem_storage(args.store))
    session = repo.readonly_session("main")
    store = session.store

    ds = xr.open_zarr(store, consolidated=False, chunks=None)
    print(f"Dataset: {len(ds.data_vars)} variables")
    print(f"Dimensions: {dict(ds.sizes)}")
    print(f"Variables: {sorted(ds.data_vars)[:10]}...")

    # --- Query 1: All refs for a surface variable ---
    # Pick a surface variable that exists
    surface_vars = [v for v in ds.data_vars if "_surface" in v]
    if surface_vars:
        var = surface_vars[0]
        refs, elapsed = timed(
            f"All refs for {var}",
            lambda: ds[var].get_object_references(),
        )
        print_refs(refs)

    # --- Query 2: Single forecast hour for a multi-level variable ---
    # Level dimensions are named level_N (e.g., level_26, level_3)
    multi_level_vars = [
        v for v in ds.data_vars
        if any(d.startswith("level_") for d in ds[v].dims)
    ]
    if multi_level_vars:
        var = multi_level_vars[0]
        fh = int(ds[var].forecast_hour[0].values)
        refs, elapsed = timed(
            f"{var}.sel(forecast_hour={fh})",
            lambda: ds[var].sel(forecast_hour=fh).get_object_references(),
        )
        print_refs(refs)

    # --- Query 3: Single level + forecast hour ---
    if multi_level_vars:
        var = multi_level_vars[0]
        level_dim = [d for d in ds[var].dims if d.startswith("level_")][0]
        fh = int(ds[var].forecast_hour[0].values)
        lv = float(ds[var][level_dim][0].values)
        refs, elapsed = timed(
            f"{var}.sel(forecast_hour={fh}, {level_dim}={lv})",
            lambda: ds[var].sel(
                forecast_hour=fh, **{level_dim: lv}
            ).get_object_references(),
        )
        print_refs(refs)

    # --- Query 4: Forecast hour range ---
    if surface_vars:
        var = surface_vars[0]
        fh_vals = ds[var].forecast_hour.values
        fh_start, fh_end = int(fh_vals[0]), int(fh_vals[min(12, len(fh_vals) - 1)])
        refs, elapsed = timed(
            f"{var}.sel(forecast_hour=slice({fh_start}, {fh_end}))",
            lambda: ds[var].sel(
                forecast_hour=slice(fh_start, fh_end)
            ).get_object_references(),
        )
        print_refs(refs)

    # --- Query 5: All refs for entire dataset ---
    refs, elapsed = timed(
        "All refs (entire dataset)",
        lambda: ds.get_object_references(),
    )
    print_refs(refs)

    print(f"\n{'=' * 60}")
    print("All queries completed without touching glacier data.")
    print("If any data had been fetched, it would have errored (files are in glacier).")


if __name__ == "__main__":
    main()
