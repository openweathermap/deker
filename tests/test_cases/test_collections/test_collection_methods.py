from datetime import datetime, timedelta, timezone
from io import BytesIO

import hdf5plugin
import pytest

from deker_local_adapters import HDF5StorageAdapter, LocalCollectionAdapter
from deker_local_adapters.storage_adapters.hdf5 import HDF5CompressionOpts, HDF5Options

from tests.parameters.collection_params import ClientParams
from tests.parameters.uri import embedded_uri

from deker import Scale
from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerInvalidManagerCallError,
    DekerInvalidSchemaError,
    DekerMemoryError,
    DekerValidationError,
)
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)
from deker.tools import get_paths
from deker.types.private.enums import SchemaType


class TestCollectionMethods:
    def test_collection_storage_adapter(self, array_collection):
        assert array_collection._storage_adapter == HDF5StorageAdapter

    def test_delete_collection(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
        storage_adapter,
    ):
        """Tests collection delete method.

        :param array_collection: Collection to delete
        """
        path = array_collection.path
        schema = path / (array_collection.name + collection_adapter.file_ext)
        assert path.exists()
        assert schema.exists()
        array = array_collection.create()
        assert array
        array_paths = get_paths(array, array_collection.path)
        main_path = array_paths.main / (array.id + storage_adapter.file_ext)
        symlink_path = array_paths.symlink / (array.id + storage_adapter.file_ext)
        assert main_path.exists()
        assert symlink_path.exists()

        array_collection.delete()

        assert not main_path.exists()
        assert not symlink_path.exists()
        assert not path.exists()
        assert not schema.exists()

    def test_clear_collection(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
        storage_adapter,
    ):
        """Test collection clears its data well.

        :param array_collection: Collection to clear
        """
        path = array_collection.path
        schema = path / (array_collection.name + collection_adapter.file_ext)
        assert path.exists()
        assert schema.exists()
        array = array_collection.create()
        assert array
        array_paths = get_paths(array, array_collection.path)
        main_path = array_paths.main / (array.id + storage_adapter.file_ext)
        symlink_path = array_paths.symlink / (array.id + storage_adapter.file_ext)
        assert main_path.exists()
        assert symlink_path.exists()

        array_collection.clear()

        assert not main_path.exists()
        assert not symlink_path.exists()
        assert path.exists()
        assert schema.exists()

    @pytest.mark.parametrize(  # noqa
        ("coll_init_params", "raised"),
        [
            (  # noqa
                {"name": None, "schema": None, "adapter": None},  # noqa
                DekerValidationError,
            ),
            (
                {
                    "name": 2,
                    "schema": ArraySchema(dimensions=[DimensionSchema("a", 10)], dtype=int),
                    "adapter": None,
                },  # noqa
                DekerValidationError,
            ),
            (
                {"name": "name", "adapter": None, "schema": None},  # noqa
                DekerValidationError,
            ),
            (
                {
                    "schema": ArraySchema(dimensions=[DimensionSchema("a", 10)], dtype=int),
                    "name": None,
                    "adapter": None,
                },  # noqa
                DekerValidationError,
            ),
            (
                {
                    "name": "name",
                    "schema": ArraySchema(dimensions=[DimensionSchema("a", 10)], dtype=int),
                    "adapter": None,
                },  # noqa
                None,
            ),
        ],
    )
    def test_collection_validate(
        self,
        coll_init_params: dict,
        raised: Exception,
        collection_adapter: LocalCollectionAdapter,
        factory,
    ):
        """Test validation of array_collection.

        :param coll_init_params: Params for array_collection initialization
        :param raised: What should be raised
        :param collection_adapter: array_collection adapter
        """
        if not raised:
            assert Collection(
                storage_adapter=collection_adapter.get_storage_adapter(),
                **{
                    **coll_init_params,
                    "adapter": collection_adapter,
                    "factory": factory,
                },
            )
        else:
            with pytest.raises(raised):  # type: ignore[call-overload]
                Collection(
                    factory=factory,
                    storage_adapter=collection_adapter.get_storage_adapter(),
                    **coll_init_params,
                )

    def test_collection_varray_schema(
        self,
        collection_adapter: LocalCollectionAdapter,
        name: str,
        factory,
    ):
        """Tests if primary attributes are created if varray_schema was passed.

        :param collection_adapter: array_collection adapter
        :param name: Name of array_collection
        """
        collection = Collection(
            name=name,
            schema=VArraySchema(dimensions=[DimensionSchema("a", 10)], vgrid=(5,), dtype=int),
            adapter=collection_adapter,
            factory=factory,
            storage_adapter=collection_adapter.get_storage_adapter(),
        )

        assert len(collection.array_schema.attributes) == 2
        assert collection.array_schema.attributes[0].name == "vid"
        assert collection.array_schema.attributes[1].name == "v_position"
        assert collection.array_schema.dimensions[0].size == 2

    @pytest.mark.parametrize(
        ("labels", "scale"),
        [
            (["label_1", "label_2", "label_3"], None),
            (None, {"start_value": 0.0, "step": 0.1}),
            (None, None),
        ],
    )
    @pytest.mark.parametrize(
        ("collection_opts", "time_dim_attr", "coll_result", "time_dim_result"),
        [
            (
                HDF5Options(
                    chunks=[1, 3],
                    compression_opts=HDF5CompressionOpts(compression="gzip", compression_opts=9),
                ),
                "$custom_attr1",
                {
                    "chunks": {"mode": "manual", "size": (1, 3)},
                    "compression": {"compression": "gzip", "options": ("9",)},
                },
                "$custom_attr1",
            ),
            (
                HDF5Options(
                    chunks=True,
                    compression_opts=HDF5CompressionOpts(compression=None, compression_opts=None),
                ),
                datetime(2023, 1, 1).replace(tzinfo=timezone.utc),
                {
                    "chunks": {"mode": "true"},
                    "compression": {"compression": "none", "options": ()},
                },
                datetime(2023, 1, 1).replace(tzinfo=timezone.utc).isoformat(),
            ),
            (
                HDF5Options(
                    chunks=None, compression_opts=HDF5CompressionOpts(**hdf5plugin.Zstd(clevel=9))
                ),
                datetime(2023, 1, 1).replace(tzinfo=timezone.utc),
                {
                    "chunks": {"mode": "none"},
                    "compression": {"compression": "32015", "options": ("9",)},
                },
                datetime(2023, 1, 1).replace(tzinfo=timezone.utc).isoformat(),
            ),
            (
                HDF5Options(
                    chunks=None,
                    compression_opts=HDF5CompressionOpts(
                        **hdf5plugin.Blosc(
                            cname="blosclz", clevel=9, shuffle=hdf5plugin.Blosc.SHUFFLE
                        )
                    ),
                ),
                datetime(2023, 1, 1).replace(tzinfo=timezone.utc),
                {
                    "chunks": {"mode": "none"},
                    "compression": {
                        "compression": "32001",
                        "options": ("0", "0", "0", "0", "9", "1", "0"),
                    },
                },
                datetime(2023, 1, 1).replace(tzinfo=timezone.utc).isoformat(),
            ),
        ],
    )
    def test_collection_as_dict(
        self,
        labels,
        scale,
        collection_opts,
        time_dim_attr,
        coll_result,
        time_dim_result,
        array_schema: ArraySchema,
        collection_adapter: LocalCollectionAdapter,
        factory,
        name: str,
    ):
        """Tests converting array_collection to dict.

        :param array_schema: Array schema
        :param collection_adapter: array_collection adapter
        :param name: Name of array_collection
        """
        collection = Collection(
            name=name,
            schema=VArraySchema(
                dimensions=[
                    DimensionSchema("dimension_1", 3, labels=labels, scale=scale),
                    TimeDimensionSchema(
                        "dimension_2",
                        3,
                        start_value=time_dim_attr,
                        step=timedelta(seconds=14400),
                    ),
                ],
                vgrid=(1, 3),
                dtype=int,
                attributes=[
                    AttributeSchema("primary_attr1", dtype=str, primary=True),
                    AttributeSchema("custom_attr1", dtype=datetime, primary=False),
                ],
            ),
            adapter=collection_adapter,
            factory=factory,
            collection_options=collection_opts,
            storage_adapter=collection_adapter.get_storage_adapter(),
        )

        assert isinstance(collection.as_dict, dict)
        assert collection.as_dict == {
            "name": collection.name,
            "type": "varray",
            "schema": {
                "dtype": "numpy.int64",
                "fill_value": str(collection.varray_schema.fill_value),
                "attributes": (
                    {"name": "primary_attr1", "dtype": "string", "primary": True},
                    {"name": "custom_attr1", "dtype": "datetime", "primary": False},
                ),
                "dimensions": (
                    {
                        "type": "generic",
                        "name": "dimension_1",
                        "size": 3,
                        "labels": labels,
                        "scale": Scale(**scale)._asdict() if scale else scale,
                    },
                    {
                        "type": "time",
                        "name": "dimension_2",
                        "size": 3,
                        "start_value": time_dim_result,
                        "step": {"days": 0, "seconds": 14400, "microseconds": 0},
                    },
                ),
                "vgrid": (1, 3),
            },
            "options": coll_result,
            "metadata_version": collection_adapter.metadata_version,
            "storage_adapter": collection_adapter.ctx.storage_adapter.__name__,
        }

    @pytest.mark.parametrize(
        "dtype",
        (
            BytesIO,
            set,
            type,
        ),
    )
    def test_wrong_dtype(self, dtype: type, array_schema: ArraySchema):
        """Checks raising exception for wrong dtypes."""
        array_schema.dtype = dtype
        with pytest.raises(DekerInvalidSchemaError):
            array_schema.as_dict

    def test_collection_prepare_json(
        self,
        array_schema: ArraySchema,
        collection_adapter: LocalCollectionAdapter,
        name: str,
        factory,
    ):
        """Test json with __schemas and name is a correct.

        :param array_schema: Array schema
        :param collection_adapter: array_collection adapter
        :param name: Name of array_collection
        """
        collection = Collection(
            name=name,
            schema=VArraySchema(dimensions=[DimensionSchema("a", 10)], vgrid=(5,), dtype=int),
            adapter=collection_adapter,
            factory=factory,
            storage_adapter=collection_adapter.get_storage_adapter(),
        )

        data = collection.as_dict

        assert isinstance(data, dict)
        assert data["type"] == SchemaType.varray.value
        assert data["schema"] == collection.varray_schema.as_dict

    def test_collection_repr(
        self,
        array_schema: ArraySchema,
        collection_adapter: LocalCollectionAdapter,
        name: str,
        factory,
    ):
        """Test of array_collection representation.

        :param array_schema: array schema
        :param collection_adapter: array_collection adapter
        :param name: name of array_collection
        :param factory: adapters factory
        """
        collection = Collection(
            name=name,
            schema=VArraySchema(dimensions=[DimensionSchema("a", 10)], vgrid=(5,), dtype=int),
            adapter=collection_adapter,
            factory=factory,
            storage_adapter=collection_adapter.get_storage_adapter(),
        )
        assert (
            repr(collection) == f"Collection(name={collection.name},"
            f" array_schema={collection.array_schema},"
            f" varray_schema={collection.varray_schema})"
        )

    def test_collection_str(self, array_collection: Collection):
        """Test correctness of name setter.

        :param array_collection: array_collection object
        """
        assert str(array_collection) == array_collection.name

    def test_create_raises_memory_error(self, client, root_path):
        """Test create raise MemoryError on too big array."""
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=10000),
                DimensionSchema(name="y", size=10000),
            ],
            dtype=float,
        )
        col_name = "memory_excess_create"
        with Client(embedded_uri(root_path), memory_limit=100, loglevel="CRITICAL") as extra_client:
            client.create_collection(col_name, schema)
            collection = extra_client.get_collection(col_name)
            try:
                with pytest.raises(DekerMemoryError):
                    collection.create()
            finally:
                collection.delete()


class TestCollectionIterator:
    def test_adapter_iter_anext(self, array_collection: Collection):
        """Tests adapter iteration over all the arrays in collection."""
        arrays = [array_collection.create() for _ in range(100)]
        assert arrays
        try:
            iterated_arrays = [a for a in array_collection]
            assert all(isinstance(a, Array) for a in iterated_arrays)
        finally:
            for a in arrays:
                a.delete()

    def test_adapter_iter(self, array_collection: Collection):
        """Tests adapter in-cycle iteration over all the arrays in array_collection."""
        arrays = [array_collection.create() for _ in range(100)]
        assert arrays
        try:
            for a in array_collection:
                assert isinstance(a, Array)
        finally:
            for a in arrays:
                a.delete()

    def test_adapter_next(self, array_collection: Collection):
        """Tests adapter iteration over all the arrays in the array_collection."""
        arrays = [array_collection.create() for _ in range(100)]
        assert arrays
        iterations = 0
        iterator = iter(array_collection)
        try:
            while True:
                a = next(iterator)
                assert isinstance(a, Array)
                iterations += 1
        except StopIteration:
            assert len(arrays) == iterations
        finally:
            for a in arrays:
                a.delete()

    @pytest.mark.parametrize(
        "params", [*ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas()]
    )
    def test_varray_collection_array_schema_creation_ok(self, params: dict, client: Client):
        """Tests if collection creates a valid array schemas, when varray schema is passed."""
        try:
            coll = client.get_collection(params["name"])
            if coll:
                coll.delete()
        except Exception:
            pass

        collection = client.create_collection(**params)
        try:
            schema = params["schema"]
            for i, dim in enumerate(schema.dimensions):
                ar_dim = collection.array_schema.dimensions[i]
                assert dim.name == ar_dim.name
                assert dim.size == ar_dim.size * schema.vgrid[i]

                if isinstance(dim, TimeDimensionSchema):
                    assert dim.step == ar_dim.step  # type: ignore[attr-defined]
                    if isinstance(dim.start_value, datetime):
                        assert dim.start_value == ar_dim.start_value  # type: ignore[attr-defined]
                    else:
                        assert (
                            f"$parent." + dim.start_value[1:] == ar_dim.start_value
                        )  # type: ignore[attr-defined]

                else:
                    assert ar_dim.labels is None  # type: ignore[attr-defined]
        finally:
            collection.delete()

    def test_collection_raise_prohibited_error(self, array_collection: Collection):
        """Check if error raised on trying to access varray manager in array collection."""
        with pytest.raises(DekerInvalidManagerCallError):
            array_collection.varrays

    def test_array_collection_return_arrays_through_manager_filter(
        self, array_collection: Collection, inserted_array: Array
    ):
        """Check if .arrays manager returns array for array collection

        :param array_collection: Array collection
        :param inserted_array: Precreated array
        """
        array = array_collection.arrays.filter({"id": inserted_array.id}).first()
        assert array.id == inserted_array.id
        assert array.primary_attributes == inserted_array.primary_attributes
        assert array.collection == inserted_array.collection
        assert array.custom_attributes == inserted_array.custom_attributes

    def test_varray_collection_return_arrays_through_array_manager_filter_by_attrs(
        self, varray_collection: Collection, varray_schema: VArraySchema, inserted_varray: VArray
    ):
        """Check if .arrays works the same way for varray collection.

        We should be able to filter arrays by id or primary attributes
        :param varray_collection:
        :param varray_schema:
        :param inserted_varray:
        :return:
        """
        position = [0] * len(varray_schema.dimensions)
        array = varray_collection.arrays.filter(
            {"vid": inserted_varray.id, "v_position": position}
        ).first()
        assert isinstance(array, Array)
        assert array.primary_attributes["vid"] == inserted_varray.id
        assert list(array.primary_attributes["v_position"]) == list(position)

    def test_varray_collection_arrays_manager_iter_return_arrays(
        self, varray_collection: Collection
    ):
        """Check if we can iterate over arrays in varray collection."""
        for array in varray_collection.arrays:
            assert isinstance(array, Array)

    def test_varray_collection_varrays_manager_iter_return_arrays(
        self, varray_collection: Collection, inserted_varray
    ):
        """Check if we can iterate over arrays in varray collection."""
        equals_to_inserted = 0
        for array in varray_collection.varrays:
            assert isinstance(array, VArray)
            if inserted_varray.id == array.id:
                equals_to_inserted += 1
        assert equals_to_inserted == 1


if __name__ == "__main__":
    pytest.main()
