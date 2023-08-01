import os

import hdf5plugin
import numpy as np
import pytest

from deker_local_adapters.storage_adapters.hdf5 import HDF5CompressionOpts, HDF5Options

from tests.parameters.common import random_string
from tests.parameters.uri import embedded_uri

from deker.client import Client
from deker.errors import DekerValidationError
from deker.schemas import ArraySchema, DimensionSchema


class TestOptions:
    @pytest.mark.parametrize(
        "chunks",
        [
            "",
            " ",
            "       ",
            "1, 2",
            False,
            0,
            -1,
            1,
            -0.1,
            0.1,
            {},
            set(),
            {1, 2, 3},
            {1: 1, 2: 2, 3: 3},
            [],
            tuple(),
            ["1", "2"],
            ("1", "2"),
        ],
    )
    def test_collection_options_chunks_validation_raises(self, chunks):
        """Test CollectionOptions chunks validation."""
        with pytest.raises(DekerValidationError):
            assert HDF5Options(chunks)

    @pytest.mark.parametrize(
        "comp_opts",
        [
            "",
            " ",
            "       ",
            "1, 2",
            True,
            False,
            0,
            -1,
            1,
            -0.1,
            0.1,
            {},
            set(),
            {1, 2, 3},
            {1: 1, 2: 2, 3: 3},
            [],
            tuple(),
            ["1", "2"],
            ("1", "2"),
        ],
    )
    def test_collection_options_compression_opts_validation_raises(self, comp_opts):
        """Test CollectionOptions compression_opts validation."""
        with pytest.raises(DekerValidationError):
            assert HDF5Options(compression_opts=comp_opts)

    @pytest.mark.parametrize(
        "compression",
        [
            "",
            " ",
            "       ",
            True,
            False,
            -0.1,
            0.1,
            {},
            set(),
            {1, 2, 3},
            {1: 1, 2: 2, 3: 3},
            [],
            tuple(),
            ["1", "2"],
            ("1", "2"),
        ],
    )
    def test_compression_opts_compression_validation_raises(self, compression):
        """Test CompressionOpts compression validation."""
        with pytest.raises(DekerValidationError):
            assert HDF5CompressionOpts(compression=compression, compression_opts=None)

    @pytest.mark.parametrize(
        ("compression", "comp_opts"),
        [
            (None, 9),
            (-1, 9),
            ("gzip", ""),
            ("gzip", " "),
            ("gzip", "       "),
            ("gzip", True),
            ("gzip", False),
            ("gzip", -1),
            ("gzip", -0.1),
            ("gzip", 0.1),
            ("gzip", {}),
            ("gzip", set()),
            ("gzip", {1, 2, 3}),
            ("gzip", {1: 1, 2: 2, 3: 3}),
        ],
    )
    def test_compression_opts_compression_opts_validation_raises(self, compression, comp_opts):
        """Test CompressionOpts compression_opts validation."""
        with pytest.raises(DekerValidationError):
            assert HDF5CompressionOpts(compression=compression, compression_opts=comp_opts)

    def test_array_is_compressed(self, client, root_path):
        dims = [DimensionSchema(name="x", size=1024), DimensionSchema(name="y", size=1024)]
        # options = HDF5Options(chunks=None, compression_opts=HDF5CompressionOpts(**hdf5plugin.Zstd(15)))
        options = HDF5Options(
            chunks=None,
            compression_opts=HDF5CompressionOpts(compression="gzip", compression_opts=9),
        )

        schema = ArraySchema(dtype=float, dimensions=dims)
        collection = client.create_collection("simple", schema=schema)
        compressed = client.create_collection(
            "compressed", schema=schema, collection_options=options
        )
        collection_path = collection.path
        compressed_path = compressed.path
        data = np.random.random((1024, 1024))

        array = collection.create()
        array_id = array.id
        array_path = collection_path / "array_symlinks" / (array_id + ".hdf5")
        array[:].update(data)

        array_c = compressed.create()
        array_c_id = array_c.id
        array_c_path = compressed_path / "array_symlinks" / (array_c_id + ".hdf5")
        array_c[:].update(data)

        assert os.path.getsize(array_path) > os.path.getsize(array_c_path)

        client.close()
        del collection
        del compressed

        with Client(str(embedded_uri(root_path))) as new_client:
            collection = new_client.get_collection("simple")
            compressed = new_client.get_collection("compressed")

            array = collection.create()
            array_id = array.id
            array_path = collection_path / "array_symlinks" / (array_id + ".hdf5")
            array[:].update(data)

            array_c = compressed.create()
            array_c_id = array_c.id
            array_c_path = compressed_path / "array_symlinks" / (array_c_id + ".hdf5")
            array_c[:].update(data)
            simple_size = os.path.getsize(array_path)
            compressed_size = os.path.getsize(array_c_path)

            assert simple_size > compressed_size

    @pytest.mark.xfail(reason="Compression filter is unavailable")
    @pytest.mark.parametrize(
        ("chunks, compression, level"),
        [
            (None, None, None),
            (True, None, None),
            ((2, 2, 2), None, None),
            ((5, 2, 1), None, None),
            ((1, 10, 10), None, None),
            ((1, 5, 2), None, None),
            ((1, 1, 1), None, None),
            (None, "gzip", 1),
            (None, "lzf", None),
            (None, "szip", ("ec", 2)),
            (True, "gzip", 9),
            (True, "lzf", None),
            (True, "szip", ("nn", 6)),
            ((2, 2, 2), "gzip", 1),
            ((2, 2, 2), "lzf", None),
            ((2, 2, 2), "szip", ("nn", 2)),
            ((1, 10, 10), "gzip", 6),
            ((1, 10, 10), "lzf", None),
            ((1, 10, 10), "szip", ("ec", 16)),
        ],
    )
    def test_collection_built_in_options(
        self,
        root_path,
        name: str,
        array_schema: ArraySchema,
        array_data: np.ndarray,
        chunks,
        compression,
        level,
    ):
        with Client(str(embedded_uri(root_path)), loglevel="ERROR") as client:
            params = HDF5Options(
                chunks, HDF5CompressionOpts(compression=compression, compression_opts=level)
            )
            collection = client.create_collection(name, array_schema, params)
            try:
                assert collection.options.as_dict == params.as_dict
                coll_from_meta = client.get_collection(name)
                assert collection.options == coll_from_meta.options
                array = collection.create()
                subset = array[:]
                subset.update(array_data)
                data = subset.read()
                assert (data == array_data).all()
            finally:
                collection.delete()

    @pytest.mark.parametrize(
        ("chunks", "compression"),
        [
            (True, hdf5plugin.Zstd(1)),
            (True, hdf5plugin.BZip2(1)),
            (True, hdf5plugin.Blosc(clevel=1)),
            (True, hdf5plugin.LZ4(1)),
            (True, hdf5plugin.SZ(1)),
            (True, hdf5plugin.Zfp()),
            (True, hdf5plugin.FciDecomp()),
            ((2, 2, 2), hdf5plugin.Zstd(9)),
            ((2, 2, 2), hdf5plugin.BZip2(9)),
            ((2, 2, 2), hdf5plugin.Blosc(clevel=9)),
            ((2, 2, 2), hdf5plugin.LZ4(9)),
            ((2, 2, 2), hdf5plugin.SZ(9)),
            ((2, 2, 2), hdf5plugin.Zfp(9)),
            ((2, 2, 2), hdf5plugin.FciDecomp()),
        ],
    )
    def test_collection_custom_options(
        self, root_path, array_schema: ArraySchema, array_data: np.ndarray, chunks, compression
    ):
        with Client(str(embedded_uri(root_path)), loglevel="ERROR") as client:
            params = HDF5Options(chunks, HDF5CompressionOpts(**compression))
            name = random_string()
            collection = client.create_collection(name, array_schema, params)
            try:
                assert collection.options.as_dict == params.as_dict
                coll_from_meta = client.get_collection(name)
                assert collection.options == coll_from_meta.options
                array = collection.create()
                subset = array[:]
                subset.update(array_data)
                data = subset.read()
                assert (data == array_data).all()
            finally:
                collection.delete()


if __name__ == "__main__":
    pytest.main()
