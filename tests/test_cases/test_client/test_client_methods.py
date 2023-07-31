import json
import os
import shutil
import string

from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any
from uuid import uuid4

import numpy as np
import pytest

from deker_local_adapters import LocalCollectionAdapter

from tests.parameters.collection_params import ClientParams
from tests.parameters.common import random_string
from tests.parameters.schemas_params import ArraySchemaParamsNoTime
from tests.parameters.uri import embedded_uri

from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerClientError,
    DekerCollectionAlreadyExistsError,
    DekerCollectionNotExistsError,
    DekerIntegrityError,
    DekerMemoryError,
    DekerValidationError,
)
from deker.log import set_logging_level
from deker.schemas import ArraySchema, DimensionSchema
from deker.tools import get_symlink_path
from deker.types import LocksExtensions
from deker.types.private.enums import LocksTypes
from deker.warnings import DekerWarning


class TestClientMethods:
    def test_storage_path_is_created_on_client_initiation(self, tmp_path_factory):
        directory = tmp_path_factory.mktemp("test_storage_creation", numbered=False)
        client = Client(str(embedded_uri(directory)))
        assert (directory / "collections").exists()
        assert [c for c in client] == []

    def test_client_open_close(self, root_path):
        """Test client open and close methods."""
        set_logging_level("DEBUG")
        client = Client(str(embedded_uri(root_path)))
        assert client.is_open
        assert not client.is_closed
        client.close()
        assert client.is_closed
        assert not client.is_open

    def test_client_context_manager(self, root_path):
        """Test client open and close methods in context manager."""
        with Client(str(embedded_uri(root_path))) as client:
            assert client.is_open
            assert not client.is_closed
        assert client.is_closed
        assert not client.is_open

    def test_client_context_manager_reopen(self, root_path):
        """Test client is reusable."""
        client = Client(str(embedded_uri(root_path)))
        assert client.is_open
        assert not client.is_closed

        client.close()
        assert client.is_closed
        assert not client.is_open

        with client:
            assert client.is_open
            assert not client.is_closed
        assert client.is_closed
        assert not client.is_open

    @pytest.mark.parametrize(
        "uri",
        [
            ":///var/tmp/collections",
            "user:pass@host:8080/data/collections/",
            "httpxs://user:pass@host:8080/data/collections/?collectionStorage=4",
            "files:///var/tmp/collections",
            "fil:///var/tmp/collections",
            "ttp://user:pass@host:8080/data/collections/",
            "htt://user:pass@host:8080/data/collections/",
            "ftp://user:pass@host:8080/data/collections/?collectionStorage=4",
        ],
    )
    def test_client_raises_invalid_uri(self, uri):
        with pytest.raises(DekerClientError):
            Client(uri)

    @pytest.mark.parametrize(
        "uri",
        [
            "http://user:pass@host:8080/data/collections/?collectionStorage=4",
            "https://user:pass@host:8080/data/collections/?collectionStorage=4",
            "cluster://user:pass@host:8080/data/collections/?collectionStorage=4",
        ],
    )
    def test_client_valid_uri_raises_no_plugin(self, uri):
        with pytest.raises(DekerClientError):
            Client(uri)

    def test_client_create_collection(
        self, client: Client, name: str, array_schema: ArraySchema
    ) -> None:
        """Tests client creates array_collection.

        :param client: Client object
        :param name:  Name of array_collection
        :param array_schema: Array schema
        """
        collection = client.create_collection(**ClientParams.ArraySchema.OK.no_vgrid_all_attrs())
        try:
            assert collection
        finally:
            collection.delete()

    def test_client_can_not_create_same_collection(
        self, client: Client, name: str, array_schema: ArraySchema
    ) -> None:
        """Tests client fails to create array_collection with the same name.

        :param client: Client object
        :param name:  Name of array_collection
        :param array_schema: Array schema
        """
        collection_params = ClientParams.ArraySchema.OK.no_vgrid_no_attrs()
        collection = client.create_collection(**collection_params)
        assert collection is not None
        try:
            with pytest.raises(DekerCollectionAlreadyExistsError):
                assert client.create_collection(**collection_params)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        "name",
        [
            None,
            True,
            False,
            "",
            " ",
            "      ",
            string.whitespace,
            1,
            0,
            -1,
            0.1,
            -0.1,
            complex(0.00000000000001),
            complex(-0.00000000000001),
            [],
            tuple(),
            dict(),
            set(),
            ["name"],
            tuple(
                "name",
            ),
            {"name"},
            {"name": "name"},
        ],
    )
    def test_client_can_not_create_collection_with_no_name(self, client: Client, name: str) -> None:
        """Tests client fails to create array_collection with no name.

        :param client: Client object
        :param name:  Name of array_collection
        """
        collection_params = ClientParams.ArraySchema.OK.no_vgrid_no_attrs()
        collection_params["name"] = name
        with pytest.raises(DekerValidationError):
            assert client.create_collection(**collection_params)

    @pytest.mark.parametrize("params", ClientParams.ArraySchema.OK.multi_array_schemas())
    def test_client_get_collection(self, client: Client, params: dict) -> None:
        """Tests reading array_collection data.

        :param client: Client object
        :param params: array_collection params
        """
        inserted_collection = client.create_collection(**params)
        collection = client.get_collection(name=inserted_collection.name)
        try:
            assert collection.name == inserted_collection.name
            assert collection.array_schema.dimensions == params["schema"].dimensions
            assert collection.array_schema.attributes == params["schema"].attributes
            assert collection.array_schema.primary_attributes == params["schema"].primary_attributes
            assert collection.array_schema.custom_attributes == params["schema"].custom_attributes
            assert collection.array_schema.shape == params["schema"].shape
            assert collection.array_schema.named_shape == params["schema"].named_shape
            assert collection.array_schema.dtype == params["schema"].dtype

            if np.isnan(params["schema"].fill_value):
                assert np.isnan(collection.array_schema.fill_value)  # type: ignore[arg-type]
            else:
                assert collection.array_schema.fill_value == params["schema"].fill_value

            assert collection.array_schema.as_dict == params["schema"].as_dict
        finally:
            inserted_collection.delete()

    @pytest.mark.parametrize("schema", ArraySchemaParamsNoTime.WRONG_params())
    def test_client_create_collection_fail(self, schema: Any, client: Client) -> None:
        """Tests that this method doesn't accept wrong schemas.

        :param client: Client object
        :param schema: any object
        """
        params = ClientParams.ArraySchema.OK.no_vgrid_no_attrs()
        with pytest.raises(DekerValidationError):
            assert client.create_collection(**{**params, "schema": schema})

    @pytest.mark.parametrize("params", ClientParams.ArraySchema.OK.multi_array_schemas())
    def test_client_iterator(self, client: Client, params: dict, root_path):
        """Tests client iterator.

        :param client: Client object
        :param params: array_collection params
        """
        shutil.rmtree(root_path)
        inserted_collection = client.create_collection(**params)
        collections = [collection for collection in client]
        try:
            assert len(collections) == 1
            assert collections[0].name == inserted_collection.name
        finally:
            for col in collections:
                col.delete()

    def test_client_iterator_empty(self, client: Client, root_path, ctx):
        """Test iterator if there is no array_collection.

        :param client: Client object
        :param root_path: Path to collections
        """
        shutil.rmtree(root_path)
        path = root_path / ctx.config.collections_directory
        if not path.resolve().exists():
            path.mkdir(parents=True, exist_ok=True)
        collections = []
        for collection in client:
            collections.append(collection)
        assert len(collections) == 0

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_client_unexpectedly_closed(self, root_path: Path, params: dict) -> None:
        """Tests closed client don't let to work with other objects' methods."""
        client = Client(embedded_uri(root_path))
        inserted_collection = client.create_collection(**params)
        client.close()
        with pytest.raises(DekerClientError):
            inserted_collection.delete()

    @pytest.mark.parametrize(
        "uri",
        [
            "",
            " ",
            "       ",
            "\n",
            "\t",
            "\r",
            None,
            "None",
            0,
            "0",
            "deker_uri",
            "some_schema://" "some_schema:///some_path",
            [],
            {},
        ],
    )
    def test_client_raises_invalid_uri_passed(self, uri):
        with pytest.raises(DekerClientError):
            Client(uri)

    def test_client_iter(self, client: Client, array_schema: ArraySchema):
        """Tests client iterator works properly."""
        names = {random_string() for _ in range(10)}
        for name in names:
            client.create_collection(name, array_schema)
        collections = [col for col in client]
        try:
            coll_names = {c.name for c in collections}
            assert names.issubset(coll_names)
            assert names - coll_names == set()
        finally:
            for col in collections:
                col.delete()

    def test_client_check_integrity_ok(self, client: Client, array_schema: ArraySchema) -> None:
        """Tests if method raises no exceptions by default."""
        collections = [
            client.create_collection(i, array_schema) for i in ["test_col1", "test_col2"]
        ]
        try:
            [col.create() for _ in range(20) for col in collections]
            client.check_integrity(2)
        finally:
            for col in collections:
                col.delete()

    def test_client_check_integrity_logfile(
        self, client: Client, array_collection_with_attributes: Collection
    ) -> None:
        """Tests if method logs errors to file."""
        array = array_collection_with_attributes.create(
            primary_attributes={"primary_attribute": 11},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )
        try:
            symlink_path = get_symlink_path(
                path_to_symlink_dir=array._Array__collection.path / array._adapter.symlinks_dir,
                primary_attributes_schema=array._Array__collection.array_schema.primary_attributes,
                primary_attributes=array.primary_attributes,
            )
            files = os.listdir(symlink_path)
            Path.unlink(symlink_path / files[0])

            filename = f"deker_integrity_report_{datetime.now().isoformat(timespec='seconds')}.txt"
            path = Path(".") / filename

            client.check_integrity(4, False, path)

            with open(path) as f:
                assert (
                    f.read()
                    == f"Collection {array_collection_with_attributes.name} arrays integrity errors:\n\t- Symlink {symlink_path} not found\n"
                )
        finally:
            array.delete()
            array_collection_with_attributes.delete()
            for root, _, files in os.walk(os.path.curdir):
                for file in files:
                    if file.startswith("deker_integrity_report_"):
                        os.remove(os.path.join(root, file))

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_client_check_integrity_raises_on_empty_file(
        self, client: Client, params: dict, collection_adapter: LocalCollectionAdapter
    ) -> None:
        """Tests if method raises exception on empty file initialization."""
        collection = client.create_collection(**params)
        filename = collection.path / (collection.name + collection_adapter.file_ext)
        open(filename, "w").close()
        with pytest.raises(JSONDecodeError):
            client.check_integrity(2)
        collection.delete()

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_client_check_integrity_collection(
        self,
        client: Client,
        params: dict,
        array_schema: ArraySchema,
        capsys,
        collection_adapter: LocalCollectionAdapter,
    ) -> None:
        """Tests if method raises exception only for collection name."""
        collection_1 = client.create_collection(**params)
        collection_2 = client.create_collection("test_collection_2", array_schema)
        filenames = [
            collection.path / (collection.name + collection_adapter.file_ext)
            for collection in [collection_1, collection_2]
        ]
        for filename in filenames:
            with open(filename, "r+") as f:
                data = json.load(f)
                data["schema"]["dtype"] = "test"
                f.seek(0)
                json.dump(data, f, indent=4)
                f.truncate()

        client.check_integrity(2, stop_on_error=False, collection=collection_1.name)
        errors = capsys.readouterr().out
        assert all(
            s in errors
            for s in (
                "Integrity check is running...\n",
                f"Collection \"{collection_1.name}\" metadata is invalid/corrupted: 'test'\n\n",
            )
        )
        collection_1.delete()
        collection_2.delete()
        for root, _, files in os.walk(os.path.curdir):
            for file in files:
                if file.startswith("deker_integrity_report_"):
                    os.remove(os.path.join(root, file))

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_client_check_integrity_raises_on_init(
        self, client: Client, params: dict, collection_adapter: LocalCollectionAdapter
    ) -> None:
        """Tests if method raises exception on collection initialization."""
        collection = client.create_collection(**params)
        filename = collection.path / (collection.name + collection_adapter.file_ext)
        with open(filename, "r+") as f:
            data = json.load(f)
            data["schema"]["dtype"] = "test"
            f.seek(0)
            json.dump(data, f, indent=4)
            f.truncate()
        with pytest.raises(DekerIntegrityError):
            client.check_integrity(1)
        collection.delete()

    @pytest.mark.parametrize(
        "options",
        [
            None,
            {
                "chunks": {"mode": "manual", "size": (1, 2, 3)},
                "compression": {"compression": "gzip", "options": ("3",)},
            },
            {
                "chunks": {"mode": "true"},
                "compression": {"compression": "32015", "options": ("2",)},
            },
        ],
    )
    def test_create_from_dict_array_collection_with_new_options(
        self, client: Client, array_collection: Collection, options
    ):
        """Check if creation from dict is correct."""
        new_col_dict = array_collection.as_dict
        new_col_dict["name"] = str(uuid4())
        new_col_dict["options"] = options

        col_from_dict = client.collection_from_dict(new_col_dict)
        # Compare through dicts
        col_dict = col_from_dict.as_dict

        assert col_dict == new_col_dict

    def test_create_from_dict_varray_collection(
        self, client: Client, varray_collection: Collection
    ):
        """Check if creation from dict is correct."""
        new_col_dict = varray_collection.as_dict
        new_col_dict["name"] = str(uuid4())

        col_from_dict = client.collection_from_dict(new_col_dict)
        # Compare through dicts
        col_dict = col_from_dict.as_dict

        del col_dict["name"]
        del new_col_dict["name"]

        assert col_dict == new_col_dict

    @pytest.mark.parametrize(
        "schema",
        [
            {
                "type": "array",
                "schema": {
                    "dtype": "numpy.int8",
                    "fill_value": "0",
                    "attributes": (
                        {"name": "primary_attr1", "dtype": "string", "primary": True},
                        {"name": "custom_attr1", "dtype": "datetime", "primary": False},
                    ),
                    "dimensions": (
                        {
                            "type": "generic",
                            "name": "dimension_1",
                            "size": 3,
                            "labels": None,
                        },
                        {
                            "type": "time",
                            "name": "dimension_2",
                            "size": 3,
                            "start_value": datetime.utcnow().isoformat(),
                            "step": {"days": 0, "seconds": 14400, "microseconds": 0},
                        },
                    ),
                },
            },
            {
                "type": "varray",
                "schema": {
                    "dtype": "numpy.int8",
                    "fill_value": "0",
                    "attributes": (
                        {"name": "primary_attr1", "dtype": "string", "primary": True},
                        {"name": "custom_attr1", "dtype": "datetime", "primary": False},
                    ),
                    "dimensions": (
                        {
                            "type": "generic",
                            "name": "dimension_1",
                            "size": 3,
                            "labels": None,
                        },
                        {
                            "type": "time",
                            "name": "dimension_2",
                            "size": 3,
                            "start_value": datetime.utcnow().isoformat(),
                            "step": {"days": 0, "seconds": 14400, "microseconds": 0},
                        },
                    ),
                    "vgrid": (1, 3),
                },
            },
        ],
    )
    def test_create_collection_with_old_metadata(self, client, schema):
        """Tests converting array_collection to dict.

        :param array_schema: Array schema
        :param collection_adapter: array_collection adapter
        :param name: Name of array_collection
        """
        old_meta = {
            "name": random_string(),
            "options": None,
            "metadata_version": "0.1",
            "storage_adapter": "HDF5StorageAdapter",
        }
        old_meta.update(schema)
        collection = client.collection_from_dict(old_meta)
        assert collection
        assert collection.as_dict["metadata_version"] == "0.2"


@pytest.mark.asyncio
class TestStorageSizeCalculation:
    def test_client_get_size_raises_non_existent_collection(self, client: Client):
        """Test if method raises on non existent collection."""
        with pytest.raises(DekerCollectionNotExistsError):
            client.calculate_storage_size("non_existent_collection")

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_get_size_empty_collection(self, client: Client, params):
        collection = client.create_collection(**params)
        try:
            with pytest.warns(DekerWarning):
                real_size = client.calculate_storage_size(collection.name)
                assert real_size.bytes == 0
                assert real_size.human == "0 B"
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("dimensions", "size", "human_size"),
        [
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=1024),
                ],
                1052810,
                "1.0 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=10240),
                ],
                10489994,
                "10.0 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=10240),
                    DimensionSchema(name="x", size=10240),
                ],
                104861834,
                "100.0 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=1024),
                    DimensionSchema(name="z", size=1024),
                ],
                1073746058,
                "1.0 GB",
            ),
        ],
    )
    def test_client_get_size_collection(self, client: Client, dimensions, size, human_size):
        """Test if method returns correct collection size."""
        schema = ArraySchema(dtype=np.int8, dimensions=dimensions)
        collection = client.create_collection(name="test_size_coll", schema=schema)
        data = np.random.randint(-128, 128, size=schema.shape, dtype=schema.dtype)  # type: ignore[arg-type]
        array = collection.create()
        array[:].update(data)
        try:
            with pytest.warns(DekerWarning):
                real_size = client.calculate_storage_size(collection.name)
                assert real_size.bytes == size
                assert real_size.human == human_size
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("dimensions", "size", "human_size"),
        [
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=1024),
                ],
                1052810,
                "4.02 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=10240),
                ],
                10489994,
                "40.02 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=10240),
                    DimensionSchema(name="x", size=10240),
                ],
                104861834,
                "400.02 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=1024),
                    DimensionSchema(name="z", size=1024),
                ],
                1073746058,
                "4.0 GB",
            ),
        ],
    )
    def test_client_get_size_full_storage(self, client: Client, dimensions, size, human_size):
        schema = ArraySchema(dtype=np.int8, dimensions=dimensions)
        data = np.random.randint(-128, 128, size=schema.shape, dtype=schema.dtype)  # type: ignore[arg-type]
        iters = 2
        collections = [
            client.create_collection(name=f"test_size_coll_{i}", schema=schema)
            for i in range(iters)
        ]
        try:
            arrays = [collection.create() for collection in collections for _ in range(iters)]
            for array in arrays:
                array[:].update(data)
            with pytest.warns(DekerWarning):
                real_size = client.calculate_storage_size()
                assert real_size.bytes == size * 4
                assert real_size.human == human_size
        finally:
            for collection in collections:
                collection.delete()


@pytest.mark.asyncio
class TestGetLocks:
    def test_client_get_locks_collection_does_not_exist(self, client: Client):
        with pytest.raises(DekerCollectionNotExistsError):
            client._get_locks("collection_does_not_exist")

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_get_locks_collection_empty(self, client: Client, params):
        collection = client.create_collection(**params)
        assert not client._get_locks(collection.name)

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_get_locks_collection_lock(self, client: Client, params):
        collection = client.create_collection(**params)
        file = collection.path.parent / (collection.name + ".lock")
        assert {
            "Lockfile": file.name,
            "Collection": file.name[: -len(LocksExtensions.collection_lock.value)],
            "Type": LocksTypes.collection_lock.value,
            "Creation": datetime.fromtimestamp(file.stat().st_ctime).isoformat(),
        } in client._get_locks()

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_get_locks_array_read_lock(
        self, client: Client, params, read_array_lock, inserted_array
    ):
        client.create_collection(**params)
        file = read_array_lock.get_path(func_args=[], func_kwargs={"array": inserted_array})
        file.touch()
        meta = file.name.split(":")
        assert {
            "Lockfile": file.name,
            "Array": meta[0],
            "ID": meta[1],
            "PID": meta[2],
            "TID": meta[3].split(".")[0],
            "Collection": inserted_array.collection,
            "Type": LocksTypes.array_read_lock.value,
            "Creation": datetime.fromtimestamp(file.stat().st_ctime).isoformat(),
        } in client._get_locks()

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_get_locks_type_array_read_lock(
        self, client: Client, params, read_array_lock, inserted_array
    ):
        client.create_collection(**params)
        file = read_array_lock.get_path(func_args=[], func_kwargs={"array": inserted_array})
        file.touch()
        meta = file.name.split(":")
        assert [
            {
                "Lockfile": file.name,
                "Array": meta[0],
                "ID": meta[1],
                "PID": meta[2],
                "TID": meta[3].split(".")[0],
                "Collection": inserted_array.collection,
                "Type": LocksTypes.array_read_lock.value,
                "Creation": datetime.fromtimestamp(file.stat().st_ctime).isoformat(),
            }
        ] == client._get_locks(lock_type=LocksTypes.array_read_lock)

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_get_locks_skip_lock(
        self, client: Client, params, read_array_lock, inserted_array
    ):
        client.create_collection(**params)
        file = read_array_lock.get_path(func_args=[], func_kwargs={"array": inserted_array})
        open(f"{file}:{os.getpid()}{LocksExtensions.varray_lock.value}", "w").close()
        assert not client._get_locks(lock_type=LocksTypes.varray_lock)


@pytest.mark.asyncio
class TestClearLocks:
    def test_client_clear_locks_collection_does_not_exist(self, client: Client):
        with pytest.raises(DekerCollectionNotExistsError):
            client.clear_locks("collection_does_not_exist")

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_clear_locks_collection_empty(self, client: Client, params):
        collection = client.create_collection(**params)
        client.clear_locks(collection.name)
        assert not list(Path.rglob(collection.path, f"*{LocksExtensions.array_read_lock.value}"))

    def test_client_clear_locks_inserted_array(
        self, client: Client, root_path, read_array_lock, inserted_array
    ):
        filepath = read_array_lock.get_path(func_args=[], func_kwargs={"array": inserted_array})
        filepath.touch()
        assert list(Path.rglob(root_path, f"*{LocksExtensions.array_read_lock.value}"))
        client.clear_locks()
        assert not list(Path.rglob(root_path, f"*{LocksExtensions.array_read_lock.value}"))

    def test_client_clear_locks_inserted_array_multiple_locks(
        self, client: Client, root_path, read_array_lock, inserted_array
    ):
        filepath = read_array_lock.get_path(func_args=[], func_kwargs={"array": inserted_array})
        open(f"{filepath}:{os.getpid()}{LocksExtensions.varray_lock.value}", "w").close()
        filepath.touch()
        assert list(Path.rglob(root_path, f"*{LocksExtensions.array_read_lock.value}"))
        assert list(Path.rglob(root_path, f"*{LocksExtensions.varray_lock.value}"))
        client.clear_locks()
        assert not list(Path.rglob(root_path, f"*{LocksExtensions.array_read_lock.value}"))
        assert not list(Path.rglob(root_path, f"*{LocksExtensions.varray_lock.value}"))

    @pytest.mark.parametrize("limit_size", [100, "1K", "10k", "25m"])
    def test_client_create_collection_raises_memory_error(self, root_path, limit_size):
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=10000),
                DimensionSchema(name="y", size=10000),
            ],
            dtype=float,
        )
        col_name = "memory_excess"
        with pytest.raises(DekerMemoryError):
            with Client(
                embedded_uri(root_path), memory_limit=limit_size, loglevel="CRITICAL"
            ) as client:
                client.create_collection(col_name, schema)

    @pytest.mark.parametrize("limit_size", [100, "1K", "10k", "25m"])
    def test_client_create_from_dict_raises_memory_error(self, root_path, client, limit_size):
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=10000),
                DimensionSchema(name="y", size=10000),
            ],
            dtype=float,
        )
        col_name = "memory_excess_dict"
        collection = client.create_collection(col_name, schema)
        coll_dict = collection.as_dict
        collection.delete()
        with pytest.raises(DekerMemoryError):
            with Client(
                embedded_uri(root_path), memory_limit=limit_size, loglevel="CRITICAL"
            ) as extra_client:
                extra_client.collection_from_dict(coll_dict)


if __name__ == "__main__":
    pytest.main()
