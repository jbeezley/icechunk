import math
from unittest.mock import MagicMock

import numpy as np

from icechunk.store import IcechunkStore


class TestIndexerToChunkBbox:
    """Test the chunk math that converts xarray indexers to bounding boxes."""

    def test_none_indexer_returns_full_range(self):
        zarr_array = MagicMock()
        zarr_array.shape = (100, 200)
        zarr_array.chunks = (10, 20)
        result = IcechunkStore._indexer_to_chunk_bbox(zarr_array, None)
        assert result == [(0, 9), (0, 9)]

    def test_slice_indexer(self):
        zarr_array = MagicMock()
        zarr_array.shape = (100,)
        zarr_array.chunks = (10,)
        indexer = MagicMock()
        indexer.tuple = (slice(15, 35),)
        result = IcechunkStore._indexer_to_chunk_bbox(zarr_array, indexer)
        # chunk 1 (10-19) through chunk 3 (30-39)
        assert result == [(1, 3)]

    def test_integer_indexer(self):
        zarr_array = MagicMock()
        zarr_array.shape = (100,)
        zarr_array.chunks = (10,)
        indexer = MagicMock()
        indexer.tuple = (25,)
        result = IcechunkStore._indexer_to_chunk_bbox(zarr_array, indexer)
        assert result == [(2, 2)]

    def test_array_indexer(self):
        zarr_array = MagicMock()
        zarr_array.shape = (100,)
        zarr_array.chunks = (10,)
        indexer = MagicMock()
        indexer.tuple = (np.array([5, 25, 45]),)
        result = IcechunkStore._indexer_to_chunk_bbox(zarr_array, indexer)
        assert result == [(0, 4)]

    def test_empty_slice(self):
        zarr_array = MagicMock()
        zarr_array.shape = (100,)
        zarr_array.chunks = (10,)
        indexer = MagicMock()
        indexer.tuple = (slice(50, 50),)
        result = IcechunkStore._indexer_to_chunk_bbox(zarr_array, indexer)
        assert result == [(0, -1)]  # empty range

    def test_multidimensional(self):
        zarr_array = MagicMock()
        zarr_array.shape = (100, 200, 50)
        zarr_array.chunks = (10, 20, 25)
        indexer = MagicMock()
        indexer.tuple = (slice(5, 25), 40, np.array([10, 30]))
        result = IcechunkStore._indexer_to_chunk_bbox(zarr_array, indexer)
        assert result == [(0, 2), (2, 2), (0, 1)]
