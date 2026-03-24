from pathlib import Path
from typing import Any

import numpy as np
import pytest

import icechunk
import zarr


@pytest.fixture
def repo(
    tmpdir: Path, any_spec_version: int | None
) -> icechunk.Repository:
    return icechunk.Repository.create(
        storage=icechunk.local_filesystem_storage(str(tmpdir)),
        spec_version=any_spec_version,
    )


def test_merge_branches_disjoint_arrays(
    repo: icechunk.Repository,
) -> None:
    """Merge two branches that modified different arrays."""
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store)
    root.create_array("array_a", shape=(10,), dtype="f8")
    root.create_array("array_b", shape=(10,), dtype="f8")
    snap0 = session.commit("initial")

    repo.create_branch("feature", snap0)

    # Write to array_a on main
    session = repo.writable_session("main")
    store = session.store
    arr_a = zarr.open_array(store=store, path="array_a", mode="r+")
    arr_a[0] = 42.0
    session.commit("main: write array_a")

    # Write to array_b on feature
    session = repo.writable_session("feature")
    store = session.store
    arr_b = zarr.open_array(store=store, path="array_b", mode="r+")
    arr_b[0] = 99.0
    session.commit("feature: write array_b")

    # Merge
    merge_snap = repo.merge_branches(
        "feature", "main", "merge feature into main"
    )

    # Verify both arrays readable from main
    session = repo.readonly_session(snapshot_id=merge_snap)
    store = session.store
    arr_a = zarr.open_array(store=store, path="array_a", mode="r")
    arr_b = zarr.open_array(store=store, path="array_b", mode="r")
    assert arr_a[0] == 42.0
    assert arr_b[0] == 99.0


def test_merge_branches_overlapping_chunks_error(
    repo: icechunk.Repository,
) -> None:
    """Merge fails when both branches write the same chunk."""
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store)
    root.create_array("array", shape=(10,), dtype="f8")
    snap0 = session.commit("initial")

    repo.create_branch("feature", snap0)

    # Both branches write chunk [0] of same array
    session = repo.writable_session("main")
    store = session.store
    arr = zarr.open_array(store=store, path="array", mode="r+")
    arr[0] = 1.0
    session.commit("main: write")

    session = repo.writable_session("feature")
    store = session.store
    arr = zarr.open_array(store=store, path="array", mode="r+")
    arr[0] = 2.0
    session.commit("feature: write")

    with pytest.raises(Exception):
        repo.merge_branches(
            "feature", "main", "conflict merge"
        )


def test_merge_branches_preserves_source(
    repo: icechunk.Repository,
) -> None:
    """Source branch is preserved after merge."""
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store)
    root.create_array("array_a", shape=(10,), dtype="f8")
    root.create_array("array_b", shape=(10,), dtype="f8")
    snap0 = session.commit("initial")

    repo.create_branch("feature", snap0)

    session = repo.writable_session("feature")
    store = session.store
    arr = zarr.open_array(store=store, path="array_b", mode="r+")
    arr[0] = 1.0
    session.commit("feature: write")
    feature_tip = repo.lookup_branch("feature")

    repo.merge_branches("feature", "main", "merge")

    assert repo.lookup_branch("feature") == feature_tip
