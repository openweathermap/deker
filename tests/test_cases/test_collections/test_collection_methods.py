import gc
import logging
import os
import sys
import threading
import time
import uuid

from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import deker_local_adapters.collection_adapter
import hdf5plugin
import pytest

from cantok import AbstractToken, SimpleToken
from deker_local_adapters import HDF5StorageAdapter, LocalArrayAdapter, LocalCollectionAdapter, LocalVArrayAdapter
from deker_local_adapters.storage_adapters.hdf5 import HDF5CompressionOpts, HDF5Options
from deker_tools.cancel import cancellation_scope
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.parameters.collection_params import ClientParams
from tests.parameters.uri import embedded_uri
from tests.test_cases.test_arrays.test_array_methods import CancelScopeAndMove, HeldEmptying

from deker import Scale
from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerCancelledError,
    DekerCriticalError,
    DekerInvalidManagerCallError,
    DekerInvalidSchemaError,
    DekerTrashError,
    DekerValidationError,
)
from deker.flock import Flock
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    SchemaTypeEnum,
    TimeDimensionSchema,
    VArraySchema,
)
from deker.tools import get_paths


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

    @pytest.mark.parametrize("array_params", [{}, {"id_": str(uuid.uuid4())}])
    def test_clear_collection(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
        storage_adapter,
        array_params,
    ):
        """Test collection clears its data well.

        :param array_collection: Collection to clear
        """
        path = array_collection.path
        schema = path / (array_collection.name + collection_adapter.file_ext)
        assert path.exists()
        assert schema.exists()
        array = array_collection.create(**array_params)
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
        ("overrides", "raised", "fails_in_factory"),
        [
            pytest.param({"name": ""}, DekerValidationError, False, id="invalid-name"),
            pytest.param({"schema": None}, DekerValidationError, False, id="invalid-schema"),
            pytest.param({}, RuntimeError, True, id="factory-fails"),
        ],
    )
    def test_collection_failed_in_constructor_is_dropped_silently(
        self,
        mocker: MockerFixture,
        collection_adapter: LocalCollectionAdapter,
        factory,
        array_schema: ArraySchema,
        name: str,
        overrides: dict,
        raised: type[Exception],
        fails_in_factory: bool,
    ):
        """Tests that a Collection whose constructor raised is destroyed without an error in its finalizer."""
        if fails_in_factory:
            # a class, not an instance: a raised instance kept by the mock would keep the collection alive
            mocker.patch.object(factory, "get_array_adapter", side_effect=RuntimeError)
        gc.collect()  # the garbage of other tests is not counted
        hook = mocker.patch.object(sys, "unraisablehook")
        params = {
            "name": name,
            "schema": array_schema,
            "adapter": collection_adapter,
            "factory": factory,
            "storage_adapter": collection_adapter.get_storage_adapter(),
            **overrides,
        }
        with pytest.raises(raised) as excinfo:
            Collection(**params)
        del excinfo
        gc.collect()
        assert [call.args[0].exc_value for call in hook.call_args_list] == []

    @pytest.mark.parametrize("schema_kind", ["array", "varray"])
    def test_valid_collection_is_dropped_silently(
        self,
        mocker: MockerFixture,
        collection_adapter: LocalCollectionAdapter,
        factory,
        array_schema: ArraySchema,
        name: str,
        schema_kind: str,
    ):
        """Tests that a valid Collection is destroyed without an error in its finalizer."""
        gc.collect()  # the garbage of other tests is not counted
        hook = mocker.patch.object(sys, "unraisablehook")
        schema = (
            array_schema
            if schema_kind == "array"
            else VArraySchema(dimensions=[DimensionSchema("a", 10)], vgrid=(5,), dtype=int)
        )
        collection = Collection(
            name=name,
            schema=schema,
            adapter=collection_adapter,
            factory=factory,
            storage_adapter=collection_adapter.get_storage_adapter(),
        )
        assert collection.name == name
        del collection
        gc.collect()
        assert [call.args[0].exc_value for call in hook.call_args_list] == []

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
                HDF5Options(chunks=None, compression_opts=HDF5CompressionOpts(**hdf5plugin.Zstd(clevel=9))),
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
                        **hdf5plugin.Blosc(cname="blosclz", clevel=9, shuffle=hdf5plugin.Blosc.SHUFFLE)
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
        [
            BytesIO,
            set,
            type,
            str,
            dict,
            tuple,
            list,
        ],
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
        assert data["type"] == SchemaTypeEnum.varray.name
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


class TestCollectionIterator:
    def test_adapter_iter_anext(self, array_collection: Collection):
        """Tests adapter iteration over all the arrays in collection."""
        arrays = [array_collection.create() for _ in range(100)]
        assert arrays
        try:
            iterated_arrays = [*array_collection]
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

    @pytest.mark.parametrize("params", [*ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas()])
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
                        assert "$parent." + dim.start_value[1:] == ar_dim.start_value  # type: ignore[attr-defined]

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
        array = varray_collection.arrays.filter({"vid": inserted_varray.id, "v_position": position}).first()
        assert isinstance(array, Array)
        assert array.primary_attributes["vid"] == inserted_varray.id
        assert list(array.primary_attributes["v_position"]) == list(position)

    def test_varray_collection_arrays_manager_iter_return_arrays(self, varray_collection: Collection):
        """Check if we can iterate over arrays in varray collection."""
        for array in varray_collection.arrays:
            assert isinstance(array, Array)

    def test_varray_collection_varrays_manager_iter_return_arrays(self, varray_collection: Collection, inserted_varray):
        """Check if we can iterate over arrays in varray collection."""
        equals_to_inserted = 0
        for array in varray_collection.varrays:
            assert isinstance(array, VArray)
            if inserted_varray.id == array.id:
                equals_to_inserted += 1
        assert equals_to_inserted == 1


if __name__ == "__main__":
    pytest.main()


class TestCollectionRepr:
    def test_repr_of_an_array_collection_names_only_the_array_schema(self, array_collection: Collection):
        assert repr(array_collection) == (
            f"Collection(name={array_collection.name}, array_schema={array_collection.array_schema})"
        )


def trash_of_collection(collection: Collection) -> Path:
    """Find the trash of the storage of a collection.

    :param collection: the collection
    :returns: the trash
    """
    return collection._Collection__adapter.trash_resource


class TestCollectionTimeoutAndCancellation:
    """The timeout of a creation is the limit of the wait for the locks it needs; the clearing and the deletion of a
    collection take no lock and have no timeout. A cancelled operation changes nothing."""

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [pytest.param({"timeout": 5}, 5, id="given"), pytest.param({}, Sentinel, id="not-given")],
    )
    def test_timeout_reaches_the_adapter(self, array_collection: Collection, mocker, kwargs, expected):
        adapter_call = mocker.patch.object(array_collection.arrays._DataManager__array_adapter, "create")
        array_collection.create(**kwargs)
        assert adapter_call.call_args.kwargs["timeout"] == expected
        assert isinstance(adapter_call.call_args.kwargs["token"], AbstractToken)

    @pytest.mark.parametrize("timeout", [0, -1, 1.5, True, "10", None])
    def test_invalid_timeout_is_rejected(self, array_collection: Collection, timeout):
        with pytest.raises(DekerValidationError):
            array_collection.create(timeout=timeout)
        assert array_collection.path.exists()

    @pytest.mark.parametrize("operation", ["create", "clear", "delete"])
    def test_cancelled_scope_changes_nothing(self, array_collection: Collection, operation):
        array = array_collection.create()
        token = SimpleToken()
        token.cancel()
        with cancellation_scope(token), pytest.raises(DekerCancelledError):
            getattr(array_collection, operation)()
        assert array_collection.path.exists()
        assert [a.id for a in array_collection] == [array.id]
        assert list(trash_of_collection(array_collection).iterdir()) == []


def files_under(root: Path) -> list[str]:
    """List the files and the symlinks under a directory, relative to it; the directories are not listed.

    :param root: directory to walk
    :returns: sorted relative paths
    """
    return sorted(
        os.path.relpath(os.path.join(directory, name), root) for directory, _, names in os.walk(root) for name in names
    )


def moved_files(trash: Path) -> list[str]:
    """List the files and the symlinks in the trash relative to what was moved: without the random name of the move.

    :param trash: the trash
    :returns: sorted relative paths
    """
    return sorted(path.partition(os.sep)[2] for path in files_under(trash))


class TestCollectionDeleteAndClear:
    """The deletion and the clearing of a collection: no lock, a move to the trash, one emptying of the trash."""

    @pytest.fixture(params=["array_collection", "varray_collection"], ids=["arrays", "varrays"])
    def target(self, request) -> Collection:
        """A collection with three arrays, one of them written, and the trash of its storage empty.

        :param request: request of the fixture
        """
        collection: Collection = request.getfixturevalue(request.param)
        for _ in range(3):
            collection.create()
        next(iter(collection))[:].update(1.0)
        assert list(trash_of_collection(collection).iterdir()) == []
        return collection

    @staticmethod
    def kept(collection: Collection, operation: str) -> list[str]:
        """List the files the operation keeps in the collection: its metadata file after the clearing, none after the
        deletion.

        :param collection: the collection
        :param operation: ``delete`` or ``clear``
        :returns: sorted relative paths
        """
        return [] if operation == "delete" else [collection.name + ".json"]

    @staticmethod
    def moved(before: list[str], collection: Collection, operation: str) -> list[str]:
        """List the files the operation moves to the trash, relative to what is moved: the directory of the
        collection for the deletion, the directories inside it for the clearing.

        :param before: the files of the collection before the operation, relative to it
        :param collection: the collection
        :param operation: ``delete`` or ``clear``
        :returns: sorted relative paths
        """
        if operation == "delete":
            return before
        return sorted(path.partition(os.sep)[2] for path in before if path != collection.name + ".json")

    def assert_done(self, collection: Collection, operation: str, ids: list[str]) -> None:
        """The collection is deleted or cleared: its arrays are found neither by the walk nor by their ids.

        :param collection: the collection
        :param operation: ``delete`` or ``clear``
        :param ids: ids of its arrays before the operation
        """
        assert files_under(collection.path) == self.kept(collection, operation)
        assert collection.path.exists() is (operation == "clear")
        if operation == "clear":
            assert list(collection) == []
            assert [array_id for array_id in ids if collection.filter({"id": array_id}).last()] == []

    @staticmethod
    def held_file_of(collection: Collection) -> Path:
        """Find the main file of an Array of the collection: the file a reader or a writer holds.

        :param collection: the collection
        :returns: the file
        """
        array = next(iter(collection.arrays))
        return array._adapter._get_main_path_to_file(array)

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("operation", ["delete", "clear"])
    @pytest.mark.parametrize("shared", [True, False], ids=["reader", "writer"])
    def test_held_file_does_not_stop_it(self, target: Collection, operation: str, shared: bool):
        ids = [array.id for array in target]
        with Flock(self.held_file_of(target), shared=shared):
            start = time.monotonic()
            assert getattr(target, operation)() is None
            assert time.monotonic() - start < 0.5  # nobody is waited for
        self.assert_done(target, operation, ids)
        assert list(trash_of_collection(target).iterdir()) == []

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    def test_trash_is_emptied_once_before_the_return_by_default(
        self, target: Collection, operation: str, mocker: MockerFixture
    ):
        ids = [array.id for array in target]
        adapter = target._Collection__adapter
        empty = mocker.spy(deker_local_adapters.collection_adapter, "empty_trash_directory")
        getattr(target, operation)()
        empty.assert_called_once_with(adapter.trash_resource, background=False, logger=adapter.logger)
        self.assert_done(target, operation, ids)
        assert list(trash_of_collection(target).iterdir()) == []

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    def test_repeated_is_not_an_error(self, target: Collection, operation: str):
        ids = [array.id for array in target]
        getattr(target, operation)()
        assert getattr(target, operation)() is None
        self.assert_done(target, operation, ids)
        assert list(trash_of_collection(target).iterdir()) == []

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    @pytest.mark.parametrize("background", [False, True])
    def test_without_emptying_the_trash_keeps_exactly_what_is_moved(
        self, target: Collection, operation: str, background: bool, mocker: MockerFixture, client: Client
    ):
        ids = [array.id for array in target]
        before = files_under(target.path)
        empty = mocker.spy(deker_local_adapters.collection_adapter, "empty_trash_directory")
        thread = mocker.spy(threading.Thread, "start")
        assert getattr(target, operation)(background=background, _empty_trash=False) is None
        empty.assert_not_called()
        thread.assert_not_called()
        self.assert_done(target, operation, ids)
        trash = trash_of_collection(target)
        assert moved_files(trash) == self.moved(before, target, operation)
        client.empty_trash()
        assert list(trash.iterdir()) == []

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("operation", ["delete", "clear"])
    def test_background_returns_while_the_trash_is_emptied(
        self, target: Collection, operation: str, mocker: MockerFixture
    ):
        ids = [array.id for array in target]
        before = files_under(target.path)
        held = HeldEmptying(deker_local_adapters.collection_adapter._empty_trash_in_background)
        mocker.patch.object(deker_local_adapters.collection_adapter, "_empty_trash_in_background", side_effect=held)
        assert getattr(target, operation)(background=True) is None
        assert held.started.wait(30)
        self.assert_done(target, operation, ids)
        trash = trash_of_collection(target)
        assert moved_files(trash) == self.moved(before, target, operation)  # returned while the emptying waits
        (thread,) = held.threads
        assert thread is not threading.current_thread()
        assert thread.daemon
        held.go.set()
        thread.join(30)
        assert list(trash.iterdir()) == []

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    def test_failed_emptying_is_raised_after_the_operation(self, target: Collection, operation: str, client: Client):
        ids = [array.id for array in target]
        trash = trash_of_collection(target)
        locked = trash / "locked"
        locked.mkdir()
        (locked / "kept").write_text("file")
        locked.chmod(0o500)
        try:
            with pytest.raises(DekerTrashError) as error:
                getattr(target, operation)()
            assert str(error.value) == (
                f"The trash of the storage is not emptied: 1 files are not removed from {trash}. "
                "Reasons: Permission denied"
            )
            self.assert_done(target, operation, ids)
            assert [path.name for path in trash.iterdir()] == ["locked"]
        finally:
            locked.chmod(0o700)
            client.empty_trash()
        assert list(trash.iterdir()) == []

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    def test_cancelled_after_the_first_move_moves_everything(
        self, target: Collection, operation: str, mocker: MockerFixture
    ):
        ids = [array.id for array in target]
        scope = SimpleToken()
        real = deker_local_adapters.collection_adapter.move_to_trash
        move = mocker.patch.object(
            deker_local_adapters.collection_adapter, "move_to_trash", side_effect=CancelScopeAndMove(scope, real)
        )
        with cancellation_scope(scope):
            assert getattr(target, operation)() is None
        assert scope.cancelled
        assert move.call_count == (1 if operation == "delete" else 4)
        self.assert_done(target, operation, ids)
        assert list(trash_of_collection(target).iterdir()) == []

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    def test_timeout_is_not_an_argument(self, target: Collection, operation: str):
        before = files_under(target.path)
        with pytest.raises(TypeError) as error:
            getattr(target, operation)(timeout=5)
        assert str(error.value) == f"Collection.{operation}() got an unexpected keyword argument 'timeout'"
        assert files_under(target.path) == before


class TestCollectionInAStorageWithoutItsDirectories:
    """The deletion or the clearing that does not find the trash or the collections directory of the storage is
    critical."""

    @pytest.fixture
    def own(self, tmp_path: Path) -> Collection:
        """A collection with an Array, in a storage of the test's own.

        :param tmp_path: directory of the test
        """
        client = Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR")
        collection = client.create_collection("own", ArraySchema(dimensions=[DimensionSchema("x", 4)], dtype=float))
        collection.create()
        yield collection
        client.close()

    @pytest.mark.parametrize("operation", ["delete", "clear"])
    @pytest.mark.parametrize("directory", ["trash", "collections"])
    def test_missing_storage_directory_is_critical(
        self, own: Collection, tmp_path: Path, mocker: MockerFixture, operation: str, directory: str
    ):
        gone = tmp_path / "storage" / directory
        gone.rename(tmp_path / "away")
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        with pytest.raises(DekerCriticalError) as error:
            getattr(own, operation)()
        assert str(error.value) == f"Storage directory {gone} doesn't exist"
        critical.assert_called_once_with(f"Storage directory {gone} doesn't exist")
        assert not gone.exists()


class TestCreateWithoutArguments:
    """Arguments not given take their defaults; an explicit ``None`` is not a value of theirs."""

    def test_create_without_arguments_gives_a_new_id_and_no_attributes(self, array_collection: Collection):
        array = array_collection.create()
        assert array.id
        assert array.primary_attributes == {}
        assert array.custom_attributes == {}

    @pytest.mark.parametrize("argument", ["primary_attributes", "custom_attributes", "id_"])
    def test_explicit_none_is_rejected(self, array_collection: Collection, argument):
        with pytest.raises(DekerValidationError):
            array_collection.create(**{argument: None})
        assert list(array_collection) == []


class MoveAwayOnSecondRead:
    """Stand for ``read_meta`` of an adapter class: the second call moves the main file away first, as a deletion
    meanwhile does; every call does the real work then.

    :param method: the real method
    :param target: where the file is moved to
    """

    def __init__(self, method, target: Path) -> None:
        self.method = method
        self.target = target
        self.calls = 0
        self.gone: list[str] = []

    def __call__(self, adapter, path: Path):
        self.calls += 1
        if self.calls == 2:
            path.rename(self.target / path.name)
            self.gone.append(path.name.split(".")[0])
        return self.method(adapter, path)


class TestIterationSkipsWhatDisappears:
    """An Array or a VArray whose main file disappears while the collection is walked is skipped."""

    @pytest.mark.parametrize(
        ("collection_", "adapter_class", "walk"),
        [
            pytest.param("array_collection", LocalArrayAdapter, "collection", id="arrays-collection"),
            pytest.param("array_collection", LocalArrayAdapter, "arrays", id="arrays-manager"),
            pytest.param("varray_collection", LocalVArrayAdapter, "collection", id="varrays-collection"),
            pytest.param("varray_collection", LocalVArrayAdapter, "varrays", id="varrays-manager"),
        ],
    )
    def test_disappeared_on_the_second_element(
        self, request, mocker, tmp_path: Path, collection_: str, adapter_class, walk: str
    ):
        collection: Collection = request.getfixturevalue(collection_)
        ids = {collection.create().id for _ in range(3)}
        read = MoveAwayOnSecondRead(adapter_class.read_meta, tmp_path)
        mocker.patch.object(adapter_class, "read_meta", autospec=True, side_effect=read)
        walked = list(collection if walk == "collection" else getattr(collection, walk))
        assert read.calls == 3
        assert len(read.gone) == 1
        assert sorted(array.id for array in walked) == sorted(ids - set(read.gone))


class TestCollectionNames:
    """The name of a collection becomes its directory: ``.``, ``..`` and names with ``/`` are refused at the creation,
    whatever way it goes; other names, dots and spaces in them included, are created as before.
    """

    SCHEMA = ArraySchema(dimensions=[DimensionSchema("x", 4)], dtype=float)

    @pytest.fixture
    def own(self, tmp_path: Path) -> Client:
        """Client of a storage of the test's own.

        :param tmp_path: directory of the test
        """
        with Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR") as client:
            yield client

    @pytest.mark.parametrize("name", [".", "..", "a/b", "a/b/c", "/abs", "a/"])
    @pytest.mark.parametrize("way", ["create_collection", "collection_from_dict"])
    def test_name_that_is_not_one_directory_is_refused(self, own: Client, tmp_path: Path, name: str, way: str):
        valid = own.create_collection("valid", self.SCHEMA)
        data = {**valid.as_dict, "name": name}
        valid.delete()
        storage = tmp_path / "storage"
        before = sorted(str(path.relative_to(storage)) for path in storage.rglob("*"))
        assert before == ["collections", "trash"]
        with pytest.raises(DekerValidationError) as error:
            if way == "create_collection":
                own.create_collection(name, self.SCHEMA)
            else:
                own.collection_from_dict(data)
        assert str(error.value) == (
            f"Collection name shall be neither '.' nor '..' and shall contain no '/', got {name!r}"
        )
        assert sorted(str(path.relative_to(storage)) for path in storage.rglob("*")) == before
        assert sorted(str(path.relative_to(tmp_path)) for path in tmp_path.iterdir()) == ["storage"]

    @pytest.mark.parametrize("name", ["a.b", "..a", "a b"])
    def test_names_with_dots_and_spaces_are_created(self, own: Client, name: str):
        collection = own.create_collection(name, self.SCHEMA)
        assert own.get_collection(name).as_dict == collection.as_dict
        assert collection.path == own._adapter.collections_resource / name
