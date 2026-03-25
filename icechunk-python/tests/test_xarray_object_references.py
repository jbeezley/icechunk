"""End-to-end tests for get_object_references via xarray + icechunk."""

from unittest.mock import MagicMock

import pytest
import zarr

from icechunk import VirtualChunkSpec, in_memory_storage
from icechunk.repository import Repository


@pytest.fixture()
def repo_with_virtual_chunks(any_spec_version):
    """Create a repo with a 1D array backed by virtual chunks."""
    repo = Repository.open_or_create(
        storage=in_memory_storage(),
        create_version=any_spec_version,
    )
    session = repo.writable_session("main")
    store = session.store

    # Create a 1D array at root: shape=(30,), chunks=(10,) → 3 chunks
    zarr.create_array(
        store,
        shape=(30,),
        chunks=(10,),
        dtype="f4",
        compressors=None,
    )

    # Set virtual refs for all 3 chunks
    store.set_virtual_refs(
        array_path="/",
        validate_containers=False,
        chunks=[
            VirtualChunkSpec(
                index=[0],
                location="s3://archive/file.nc",
                offset=0,
                length=1000,
            ),
            VirtualChunkSpec(
                index=[1],
                location="s3://archive/file.nc",
                offset=1000,
                length=1000,
            ),
            VirtualChunkSpec(
                index=[2],
                location="s3://archive/other.nc",
                offset=0,
                length=1000,
            ),
        ],
    )

    session.commit("add virtual array")
    return repo


def test_store_get_object_references_all(repo_with_virtual_chunks):
    """get_object_references returns all virtual chunk refs."""
    session = repo_with_virtual_chunks.readonly_session("main")
    store = session.store

    zarr_array = zarr.open_array(store=store, mode="r")
    refs = store.get_object_references(zarr_array)

    assert len(refs) == 3
    uris = [r.uri for r in refs]
    assert "s3://archive/file.nc" in uris
    assert "s3://archive/other.nc" in uris

    # Check byte ranges are populated
    for ref in refs:
        assert ref.byte_offset is not None
        assert ref.byte_length is not None


def test_store_get_object_references_with_indexer(repo_with_virtual_chunks):
    """get_object_references with indexer filters to selected chunks."""
    session = repo_with_virtual_chunks.readonly_session("main")
    store = session.store

    zarr_array = zarr.open_array(store=store, mode="r")

    # Create indexer for first 10 elements (chunk 0 only)
    indexer = MagicMock()
    indexer.tuple = (slice(0, 10),)

    refs = store.get_object_references(zarr_array, indexer)

    assert len(refs) == 1
    assert refs[0].uri == "s3://archive/file.nc"
    assert refs[0].byte_offset == 0
    assert refs[0].byte_length == 1000


def test_store_get_object_references_uncommitted(any_spec_version):
    """get_object_references works for uncommitted virtual chunks."""
    repo = Repository.open_or_create(
        storage=in_memory_storage(),
        create_version=any_spec_version,
    )
    session = repo.writable_session("main")
    store = session.store

    zarr.create_array(
        store,
        shape=(10,),
        chunks=(5,),
        dtype="f4",
        compressors=None,
    )

    store.set_virtual_refs(
        array_path="/",
        validate_containers=False,
        chunks=[
            VirtualChunkSpec(
                index=[0],
                location="s3://bucket/uncommitted.nc",
                offset=42,
                length=999,
            ),
        ],
    )

    # Don't commit — should still return refs from changeset
    zarr_array = zarr.open_array(store=store, mode="r")
    refs = store.get_object_references(zarr_array)

    assert len(refs) == 1
    assert refs[0].uri == "s3://bucket/uncommitted.nc"
    assert refs[0].byte_offset == 42
    assert refs[0].byte_length == 999
