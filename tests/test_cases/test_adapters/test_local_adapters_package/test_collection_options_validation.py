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

    def test_array_is_compressed(self, root_path):
        """Tests if a compressed collection stores smaller files, in the client that created it and in a new one.

        The test opens its own clients: closing the shared client fixture would break the neighbouring tests.
        """
        dims = [DimensionSchema(name="x", size=1024), DimensionSchema(name="y", size=1024)]
        options = HDF5Options(
            chunks=None,
            compression_opts=HDF5CompressionOpts(compression="gzip", compression_opts=9),
        )
        schema = ArraySchema(dtype=float, dimensions=dims)
        data = np.random.random((1024, 1024))

        with Client(str(embedded_uri(root_path))) as first_client:
            collection = first_client.create_collection("simple", schema=schema)
            compressed = first_client.create_collection("compressed", schema=schema, collection_options=options)
            collection_path = collection.path
            compressed_path = compressed.path

            array = collection.create()
            array_path = collection_path / "array_symlinks" / (array.id + ".hdf5")
            array[:].update(data)

            array_c = compressed.create()
            array_c_path = compressed_path / "array_symlinks" / (array_c.id + ".hdf5")
            array_c[:].update(data)

            assert os.path.getsize(array_path) > os.path.getsize(array_c_path)

        with Client(str(embedded_uri(root_path))) as new_client:
            collection = new_client.get_collection("simple")
            compressed = new_client.get_collection("compressed")
            try:
                array = collection.create()
                array_path = collection_path / "array_symlinks" / (array.id + ".hdf5")
                array[:].update(data)

                array_c = compressed.create()
                array_c_path = compressed_path / "array_symlinks" / (array_c.id + ".hdf5")
                array_c[:].update(data)

                assert os.path.getsize(array_path) > os.path.getsize(array_c_path)
            finally:
                collection.delete()
                compressed.delete()

    @pytest.mark.xfail(reason="Compression filter is unavailable")
    @pytest.mark.parametrize(
        ("chunks", "compression", "level"),
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
        client: Client,
        name: str,
        array_schema: ArraySchema,
        array_data: np.ndarray,
        chunks,
        compression,
        level,
    ):
        params = HDF5Options(chunks, HDF5CompressionOpts(compression=compression, compression_opts=level))
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
        self, client: Client, array_schema: ArraySchema, array_data: np.ndarray, chunks, compression
    ):
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


class TestHDF5OptionsRepr:
    def test_compression_opts_repr_and_dict(self):
        opts = HDF5CompressionOpts(compression="gzip", compression_opts=9)
        assert opts.as_dict == {"compression": "gzip", "compression_opts": 9}
        assert repr(opts) == "HDF5CompressionOpts(compression=gzip, compression_opts=9)"
        assert str(opts) == repr(opts)

    @pytest.mark.parametrize("kwargs", [{}, {"compression_opts": None}])
    def test_options_repr_and_default_compression(self, kwargs):
        opts = HDF5Options(chunks=True, **kwargs)
        assert opts.compression_opts == HDF5CompressionOpts()
        assert opts.compression_opts == HDF5CompressionOpts(compression=None, compression_opts=None)
        assert repr(opts).startswith("HDF5Options(chunks=True, compression_opts=HDF5CompressionOpts(")
        assert str(opts) == repr(opts)


class TestHDF5OptionsProcessing:
    """``_process_options`` turns the dict stored in the collection meta back into HDF5Options parameters."""

    @pytest.mark.parametrize(
        ("stored", "expected"),
        [
            ({"compression": {"compression": "none", "options": ()}}, (None, None)),
            ({"compression": {"compression": "gzip", "options": ("9",)}}, ("gzip", 9)),
            ({"compression": {"compression": "gzip", "options": ()}}, ("gzip", None)),
            ({"compression": {"compression": "szip", "options": ("ec", "8")}}, ("szip", ("ec", 8))),
            ({"compression": {"compression": "szip", "options": ()}}, ("szip", None)),
            ({"compression": {"compression": "lzf", "options": ()}}, ("lzf", None)),
            (
                {"compression": {"compression": "32001", "options": ("0", "0", "0", "0", "5", "1", "1")}},
                (32001, (0, 0, 0, 0, 5, 1, 1)),
            ),
            ({"compression": {"compression": None, "options": ()}}, (None, None)),
        ],
    )
    def test_compression_is_restored(self, stored, expected):
        params = HDF5Options._process_options(stored)
        opts = params["compression_opts"]
        assert (opts.compression, opts.compression_opts) == expected

    def test_lzf_does_not_accept_options(self):
        with pytest.raises(ValueError, match="LZF"):
            HDF5Options._process_options({"compression": {"compression": "lzf", "options": ("1",)}})

    def test_gzip_level_must_be_an_integer(self):
        with pytest.raises(DekerValidationError, match="gzip"):
            HDF5Options._process_options({"compression": {"compression": "gzip", "options": ("fast",)}})

    @pytest.mark.parametrize(
        ("stored", "expected"),
        [
            ({"chunks": None}, None),
            ({"chunks": {"mode": "none", "size": None}}, None),
            ({"chunks": {"mode": "true", "size": None}}, True),
            ({"chunks": {"mode": "manual", "size": (10, 10)}}, (10, 10)),
        ],
    )
    def test_chunks_are_restored(self, stored, expected):
        assert HDF5Options._process_options(stored)["chunks"] == expected

    def test_empty_options(self):
        assert HDF5Options._process_options(None) == {}
        assert HDF5Options._process_options({}) == {}


if __name__ == "__main__":
    pytest.main()
