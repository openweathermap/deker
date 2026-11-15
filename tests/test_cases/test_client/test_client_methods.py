import json
import logging
import os
import shutil
import string

from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import numpy as np
import pytest

from deker_local_adapters import LocalCollectionAdapter
from deker_tools.data import convert_size_to_human

from tests.parameters.collection_params import ClientParams
from tests.parameters.common import random_string
from tests.parameters.schemas_params import ArraySchemaParamsNoTime
from tests.parameters.uri import embedded_uri

from deker.client import Client
from deker.collection import Collection
from deker.ctx import CTX
from deker.errors import (
    DekerClientError,
    DekerCollectionAlreadyExistsError,
    DekerCollectionNotExistsError,
    DekerIntegrityError,
    DekerMemoryError,
    DekerMetaDataError,
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
        assert [*client] == []

    def test_client_ctx_follows_client_state(self, root_path):
        """Tests if the client context is available and reflects the client state."""
        client = Client(str(embedded_uri(root_path)))
        assert isinstance(client._ctx, CTX)
        assert not client._ctx.is_closed
        client.close()
        assert client._ctx.is_closed

    @pytest.mark.parametrize(
        "call",
        [
            lambda client: client.get_collection("x"),
            lambda client: client.create_collection(
                "x", ArraySchema(dtype=int, dimensions=[DimensionSchema(name="d", size=1)])
            ),
            lambda client: client.collection_from_dict({}),
            lambda client: client.meta_version,
            lambda client: client.root_path,
            lambda client: client.calculate_storage_size(),
            lambda client: client.clear_locks(),
            lambda client: client.check_integrity(),
            lambda client: list(client),
        ],
    )
    def test_closed_client_raises_its_own_error(self, root_path, call):
        """Tests if every call of a closed client answers with a plain DekerClientError, without a traceback chain."""
        client = Client(str(embedded_uri(root_path)))
        client.close()
        with pytest.raises(DekerClientError) as info:
            call(client)
        assert type(info.value) is DekerClientError
        assert info.value.__cause__ is None
        assert info.value.__suppress_context__ or info.value.__context__ is None

    def test_iteration_over_a_client_with_a_closed_adapter_raises_its_own_error(self, root_path):
        """The adapter closed by its context answers with its error; the client translates it without a chain."""
        client = Client(str(embedded_uri(root_path)))
        client._ctx.is_closed = True
        with pytest.raises(DekerClientError, match="Client is closed") as info:
            list(client)
        assert info.value.__suppress_context__

    def test_client_without_installed_adapters_raises(self, root_path):
        """No deker_*_adapters package around: the client says what to install instead of failing on a scheme."""
        with patch.object(Client, "_Client__plugins", {}), patch("deker.client.pkgutil.iter_modules", return_value=[]):
            with pytest.raises(DekerClientError, match="No installed adapters are found"):
                Client(str(embedded_uri(root_path)))

    @pytest.mark.parametrize(
        "metadata",
        [
            pytest.param({"name": "newer", "metadata_version": "99.0"}, id="newer-version"),
            pytest.param({"name": "no-version"}, id="no-version"),
        ],
    )
    def test_collection_metadata_of_an_unsupported_version_raises(self, client: Client, metadata: dict):
        with pytest.raises(DekerMetaDataError, match="metadata version is not supported"):
            client.collection_from_dict(metadata)
        assert client.get_collection(metadata["name"]) is None

    def test_collection_from_old_metadata_gets_the_defaults(self, client: Client):
        """Metadata of older versions has no options, no attributes, no fill_value and bare dimensions."""
        name = random_string()
        old_meta = {
            "name": name,
            "metadata_version": "0.1",
            "storage_adapter": "HDF5StorageAdapter",
            "type": "array",
            "schema": {
                "dtype": "numpy.int8",
                "dimensions": [
                    {"type": "generic", "name": "x", "size": 3},
                    {"type": "generic", "name": "y", "size": 2},
                ],
            },
        }
        collection = client.collection_from_dict(old_meta)
        try:
            assert collection.options is None
            schema = collection.array_schema
            assert schema.attributes == ()
            assert schema.fill_value == np.iinfo(np.int8).min
            assert [dim.labels for dim in schema.dimensions] == [None, None]
            assert [dim.scale for dim in schema.dimensions] == [None, None]
            assert schema.shape == (3, 2)
        finally:
            collection.delete()

    def test_adapter_error_of_closed_client_is_translated(self, root_path):
        """Tests if an adapter closed by its context is answered by the client with its own error."""
        client = Client(str(embedded_uri(root_path)))
        client._ctx.is_closed = True  # the context is closed, the client itself does not know it yet
        with pytest.raises(DekerClientError) as info:
            client.get_collection("x")
        assert type(info.value) is DekerClientError
        assert info.value.__suppress_context__

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

    def test_client_create_collection(self, client: Client, name: str, array_schema: ArraySchema) -> None:
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

    def test_client_can_not_create_same_collection(self, client: Client, name: str, array_schema: ArraySchema) -> None:
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
        collections = [*client]
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
            "some_schema://some_schema:///some_path",
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
        collections = [*client]
        try:
            coll_names = {c.name for c in collections}
            assert names.issubset(coll_names)
            assert names - coll_names == set()
        finally:
            for col in collections:
                col.delete()

    def test_client_check_integrity_ok(
        self, client: Client, array_schema: ArraySchema, deker_log: pytest.LogCaptureFixture
    ) -> None:
        """Tests if method raises no exceptions by default and reports no errors to the log."""
        collections = [client.create_collection(i, array_schema) for i in ["test_col1", "test_col2"]]
        try:
            [col.create() for _ in range(20) for col in collections]
            client.check_integrity(2)
            assert "Integrity check is running..." in deker_log.messages
            assert "Integrity check found no errors" in deker_log.messages
        finally:
            for col in collections:
                col.delete()

    @pytest.mark.parametrize("as_type", [Path, str], ids=["path", "string"])
    def test_client_check_integrity_logfile(
        self, client: Client, array_collection_with_attributes: Collection, tmp_path: Path, as_type: type
    ) -> None:
        """Tests if method logs errors to a file given as a Path or as a string."""
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
            path = tmp_path / filename

            client.check_integrity(4, False, as_type(path))

            with open(path) as f:
                assert f.read() == (
                    f"Collection {array_collection_with_attributes.name} arrays integrity errors:"
                    f"\n\t- Symlink {symlink_path} not found\n"
                )
        finally:
            array.delete()
            array_collection_with_attributes.delete()

    def test_client_check_integrity_without_a_file_only_logs(
        self,
        client: Client,
        array_collection_with_attributes: Collection,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        deker_log: pytest.LogCaptureFixture,
    ) -> None:
        """Tests if ``to_file=False`` logs the errors and writes no report anywhere."""
        monkeypatch.chdir(tmp_path)
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
            Path.unlink(symlink_path / os.listdir(symlink_path)[0])

            client.check_integrity(4, False, False)

            assert not list(tmp_path.glob("deker_integrity_report_*.txt"))
            errors = [record for record in deker_log.records if record.levelno == logging.ERROR]
            assert len(errors) == 1
            assert errors[0].getMessage() == (
                f"Collection {array_collection_with_attributes.name} arrays integrity errors:"
                f"\n\t- Symlink {symlink_path} not found\n"
            )
        finally:
            array.delete()
            array_collection_with_attributes.delete()

    def test_client_check_integrity_default_logfile(
        self,
        client: Client,
        array_collection_with_attributes: Collection,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        deker_log: pytest.LogCaptureFixture,
    ) -> None:
        """Tests if ``to_file=True`` writes a default-named report to the current directory and logs it."""
        monkeypatch.chdir(tmp_path)
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
            Path.unlink(symlink_path / os.listdir(symlink_path)[0])

            client.check_integrity(4, False, True)

            reports = list(tmp_path.glob("deker_integrity_report_*.txt"))
            assert len(reports) == 1
            expected = (
                f"Collection {array_collection_with_attributes.name} arrays integrity errors:"
                f"\n\t- Symlink {symlink_path} not found\n"
            )
            assert reports[0].read_text() == expected
            errors = [record for record in deker_log.records if record.levelno == logging.ERROR]
            assert len(errors) == 1
            assert errors[0].getMessage() == f"{expected}\n\nIntegrity check logged errors in {reports[0].absolute()}"
        finally:
            array.delete()
            array_collection_with_attributes.delete()

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
        try:
            client.check_integrity(2, stop_on_error=False, collection=collection_1.name)
        except Exception as e:
            assert str(e) == f"Collection \"{collection_1.name}\" metadata is invalid/corrupted: 'test'"
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

    @pytest.mark.parametrize(
        ("splitter", "expected"),
        [
            ({"vgrid": [1, 3]}, (3, 1)),
            ({"arrays_shape": [1, 3]}, (3, 1)),
            ({"vgrid": [3, 1]}, (1, 3)),
            ({"arrays_shape": [3, 1]}, (1, 3)),
        ],
    )
    def test_create_from_dict_varray_collection(
        self, client: Client, varray_collection: Collection, splitter: dict, expected: tuple
    ):
        """Check if creation from dict is correct."""
        schema = {
            "metadata_version": "0.2",
            "name": "varray_from_dict_test_collection",
            "type": "varray",
            "schema": {
                "dtype": "int",
                "fill_value": -1,
                "attributes": [
                    {"name": "primary_attr_1", "dtype": "string", "primary": True},
                    {"name": "custom_attr_1", "dtype": "datetime", "primary": False},
                ],
                "dimensions": [
                    {
                        "type": "generic",
                        "name": "dimension_1",
                        "size": 3,
                        "labels": ["label_1", "label_2", "label_3"],
                    },
                    {
                        "type": "time",
                        "name": "dimension_2",
                        "size": 3,
                        "start_value": "$custom_attr_1",
                        "step": {"days": 0, "seconds": 14400, "microseconds": 0},
                    },
                ],
            },
            "options": {
                "chunks": {"mode": "manual", "size": [1, 3]},
                "compression": {"compression": "gzip", "options": [9]},
            },
        }
        col_from_dict = None

        try:
            schema["schema"].update(**splitter)
            col_from_dict = client.collection_from_dict(schema)
            assert col_from_dict
            assert col_from_dict.name == schema["name"]
            if list(splitter.keys())[0] == "vgrid":
                other_splitter = "arrays_shape"
            else:
                other_splitter = "vgrid"
            assert getattr(col_from_dict.varray_schema, other_splitter) == expected
        finally:
            if col_from_dict:
                col_from_dict.delete()

    def test_create_from_other_varray_collection_dict(self, client: Client, varray_collection: Collection):
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
    def test_client_get_size_counts_only_the_storage_files(self, client: Client):
        """A stray file in the collection directory (a report, a note) is not part of the storage size."""
        schema = ArraySchema(dtype=np.int8, dimensions=[DimensionSchema(name="x", size=8)])
        collection = client.create_collection("size_with_a_stray_file", schema)
        try:
            array = collection.create()
            array[:].update(np.ones(8, dtype=np.int8))
            size_before = client.calculate_storage_size(collection.name).bytes
            assert size_before > 0
            stray = client.root_path / collection.name / "notes.txt"
            stray.write_text("x" * 4096)
            assert client.calculate_storage_size(collection.name).bytes == size_before
        finally:
            collection.delete()

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
                1052777,
                "1.0 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=10240),
                ],
                10489961,
                "10.0 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=10240),
                    DimensionSchema(name="x", size=10240),
                ],
                104861801,
                "100.0 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=1024),
                    DimensionSchema(name="z", size=1024),
                ],
                1073746025,
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
                1052777,
                "4.02 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=10240),
                ],
                10489961,
                "40.02 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=10240),
                    DimensionSchema(name="x", size=10240),
                ],
                104861801,
                "400.02 MB",
            ),
            (
                [
                    DimensionSchema(name="y", size=1024),
                    DimensionSchema(name="x", size=1024),
                    DimensionSchema(name="z", size=1024),
                ],
                1073746025,
                "4.0 GB",
            ),
        ],
    )
    def test_client_get_size_full_storage(self, client: Client, dimensions, size, human_size):
        schema = ArraySchema(dtype=np.int8, dimensions=dimensions)
        data = np.random.randint(-128, 128, size=schema.shape, dtype=schema.dtype)  # type: ignore[arg-type]
        iters = 2
        # the storage is shared by the whole test session: measure the growth, not the absolute size
        with pytest.warns(DekerWarning):
            before = client.calculate_storage_size().bytes
        collections = [client.create_collection(name=f"test_size_coll_{i}", schema=schema) for i in range(iters)]
        try:
            arrays = [collection.create() for collection in collections for _ in range(iters)]
            for array in arrays:
                array[:].update(data)
            with pytest.warns(DekerWarning):
                real_size = client.calculate_storage_size()
                assert real_size.bytes - before == size * 4
                assert convert_size_to_human(size * 4) == human_size
        finally:
            for collection in collections:
                collection.delete()


def legacy_marker(filepath: Path, pid: int, token: str) -> Path:
    """Path of a VArray write marker as Deker < 2 made it next to the main file of a tile.

    :param filepath: main file of the tile
    :param pid: PID of the writing process
    :param token: token of the writing process
    :returns: path of the marker
    """
    return filepath.parent / f"{filepath.name}:{pid}:{token}{LocksExtensions.varray_lock.value}"


@pytest.mark.asyncio
class TestGetLocks:
    """The listing of the lock files on the disk: leftovers of older versions and collections being created."""

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
        """Tests the lock file of a collection, which stays as its marker, is listed as a collection lock."""
        collection = client.create_collection(**params)
        file = collection.path.parent / (collection.name + LocksExtensions.collection_lock.value)
        expected = {
            "Lockfile": file.name,
            "Collection": collection.name,
            "Type": LocksTypes.collection_lock.value,
            "Creation": datetime.fromtimestamp(file.stat().st_ctime).isoformat(),
        }
        assert expected in client._get_locks()
        assert expected in client._get_locks(lock_type=LocksTypes.collection_lock)
        assert expected not in client._get_locks(lock_type=LocksTypes.varray_lock)

    def test_client_get_locks_lists_no_directories(self, client: Client, inserted_array):
        """Tests the directories of the collections are never taken for lock files."""
        assert not [lock for lock in client._get_locks() if lock["Lockfile"] == inserted_array.collection]

    def test_client_get_locks_varray_lock(self, client: Client, write_array_lock, inserted_array):
        """Tests a marker of a VArray write left by Deker < 2 is listed with the array, the PID and the token."""
        write_array_lock.args = []
        write_array_lock.kwargs = {"array": inserted_array}
        file = legacy_marker(write_array_lock.get_path(), 1, "0" * 32)
        file.touch()
        try:
            expected = {
                "Lockfile": file.name,
                "Array": inserted_array.id,
                "PID": "1",
                "Token": "0" * 32,
                "Collection": inserted_array.collection,
                "Type": LocksTypes.varray_lock.value,
                "Creation": datetime.fromtimestamp(file.stat().st_ctime).isoformat(),
            }
            assert expected in client._get_locks()
            assert [expected] == client._get_locks(lock_type=LocksTypes.varray_lock)
            assert expected in client._get_locks(collection_name=inserted_array.collection)
            assert expected not in client._get_locks(lock_type=LocksTypes.collection_lock)
        finally:
            file.unlink()

    @pytest.mark.parametrize(
        "stray_name",
        [
            pytest.param("old-format.varraylock", id="no-pid-and-token"),
            pytest.param("file.hdf5:123.varraylock", id="no-token"),
            pytest.param("file.hdf5:1:2:3.varraylock", id="too-many-parts"),
        ],
    )
    def test_client_get_locks_skips_markers_of_another_format(
        self, client: Client, write_array_lock, inserted_array, stray_name: str
    ):
        """A marker that is not <file>:<pid>:<token> cannot be described and is left out of the listing."""
        write_array_lock.args = []
        write_array_lock.kwargs = {"array": inserted_array}
        marker = legacy_marker(write_array_lock.get_path(), 1, "0" * 32)
        stray = marker.parent / stray_name
        marker.touch()
        stray.touch()
        try:
            listed = client._get_locks(collection_name=inserted_array.collection)
            assert [lock["Lockfile"] for lock in listed] == [marker.name]
        finally:
            marker.unlink()
            stray.unlink()

    def test_client_get_locks_lists_nothing_during_a_read(self, client: Client, inserted_array):
        """Tests a read leaves no lock files to list: the read lock lives in the kernel."""
        inserted_array[:].read()
        assert not [lock for lock in client._get_locks() if lock["Type"] != LocksTypes.collection_lock.value]

    def test_client_get_locks_lists_nothing_after_a_varray_write(self, client: Client, inserted_varray):
        """Tests a VArray write leaves no lock files to list: the tiles are held in the kernel."""
        inserted_varray[:].update(1.0)
        assert not client._get_locks(collection_name=inserted_varray.collection)


@pytest.mark.asyncio
class TestClearLocks:
    """``clear_locks`` is kept for the markers left by Deker < 2 and is removed in 3.0.0."""

    def test_client_clear_locks_collection_does_not_exist(self, client: Client):
        with pytest.warns(DeprecationWarning, match="3.0.0"), pytest.raises(DekerCollectionNotExistsError):
            client.clear_locks("collection_does_not_exist")

    @pytest.mark.parametrize(
        "params",
        [
            *ClientParams.ArraySchema.OK.multi_array_schemas(),
            *ClientParams.VArraySchema.OK.OK_params_multi_varray_schemas(),
        ],
    )
    def test_client_clear_locks_collection_empty(self, client: Client, params, deker_log: pytest.LogCaptureFixture):
        collection = client.create_collection(**params)
        with pytest.warns(DeprecationWarning, match="3.0.0"):
            client.clear_locks(collection.name)
        assert not list(Path.rglob(collection.path, f"*{LocksExtensions.varray_lock.value}"))
        assert "0 lock files removed" in deker_log.messages

    def test_client_clear_locks_removes_legacy_varray_markers(
        self, client: Client, root_path, write_array_lock, inserted_array, deker_log: pytest.LogCaptureFixture
    ):
        """Tests clear_locks removes the markers of VArray writes left by Deker < 2, whatever process made them."""
        write_array_lock.kwargs = {"array": inserted_array}
        filepath = write_array_lock.get_path()
        legacy_marker(filepath, os.getpid(), uuid4().hex).touch()
        legacy_marker(filepath, os.getpid() + 1, uuid4().hex).touch()
        assert len(list(Path.rglob(root_path, f"*{LocksExtensions.varray_lock.value}"))) == 2
        with pytest.warns(DeprecationWarning, match="3.0.0"):
            client.clear_locks()
        assert not list(Path.rglob(root_path, f"*{LocksExtensions.varray_lock.value}"))
        assert "2 lock files removed" in deker_log.messages

    def test_client_clear_locks_keeps_the_data_files(self, client: Client, root_path, inserted_array):
        """Tests clear_locks touches nothing but the markers: the data files stay."""
        before = sorted(path for path in Path.rglob(root_path, "*") if path.is_file())
        with pytest.warns(DeprecationWarning, match="3.0.0"):
            client.clear_locks()
        assert sorted(path for path in Path.rglob(root_path, "*") if path.is_file()) == before

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
            with Client(embedded_uri(root_path), memory_limit=limit_size, loglevel="CRITICAL") as client:
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
            with Client(embedded_uri(root_path), memory_limit=limit_size, loglevel="CRITICAL") as extra_client:
                extra_client.collection_from_dict(coll_dict)


if __name__ == "__main__":
    pytest.main()
