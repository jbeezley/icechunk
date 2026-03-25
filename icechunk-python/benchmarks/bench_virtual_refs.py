"""Benchmark: batch get_virtual_chunk_references vs per-chunk approach.

Run: uv run python benchmarks/bench_virtual_refs.py
"""

import time

import zarr

from icechunk import VirtualChunkSpec, in_memory_storage
from icechunk.repository import Repository


def create_repo_with_n_virtual_chunks(n_chunks: int, chunk_size: int = 100):
    """Create a repo with a 1D array of n virtual chunks."""
    repo = Repository.open_or_create(storage=in_memory_storage())
    session = repo.writable_session("main")
    store = session.store

    shape = (n_chunks * chunk_size,)
    zarr.create_array(
        store, shape=shape, chunks=(chunk_size,), dtype="f4", compressors=None
    )

    chunks = [
        VirtualChunkSpec(
            index=[i],
            location=f"s3://archive/file_{i % 10}.nc",
            offset=i * 1000,
            length=1000,
        )
        for i in range(n_chunks)
    ]
    store.set_virtual_refs(
        array_path="/", validate_containers=False, chunks=chunks
    )
    session.commit("add virtual chunks")
    return repo


def bench_batch(repo, n_chunks: int):
    """Time the batch get_virtual_chunk_references approach."""
    session = repo.readonly_session("main")
    store = session.store
    zarr_array = zarr.open_array(store=store, mode="r")

    start = time.perf_counter()
    refs = store.get_object_references(zarr_array)
    elapsed = time.perf_counter() - start

    return len(refs), elapsed


def main():
    print("Benchmarking get_virtual_chunk_references (batch Rust path)")
    print("=" * 60)

    for n_chunks in [10, 100, 1000, 5000]:
        repo = create_repo_with_n_virtual_chunks(n_chunks)
        n_refs, elapsed = bench_batch(repo, n_chunks)
        rate = n_refs / elapsed if elapsed > 0 else float("inf")
        print(
            f"  {n_chunks:>5} chunks: {n_refs:>5} refs in {elapsed:.4f}s "
            f"({rate:.0f} refs/s)"
        )

    print()
    print("Done.")


if __name__ == "__main__":
    main()
