import gc
import json
import logging
import os
import shutil
import string

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from multiprocessing import cpu_count
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import numpy as np
import pytest

from cantok import AbstractToken, SimpleToken
from deker_local_adapters import LocalCollectionAdapter
from deker_local_adapters.errors import DekerStorageError
from deker_tools.cancel import cancellation_scope
from deker_tools.data import convert_size_to_human
from deker_tools.typings import Sentinel

from tests.parameters.collection_params import ClientParams
from tests.parameters.common import random_string
from tests.parameters.schemas_params import ArraySchemaParamsNoTime
from tests.parameters.uri import embedded_uri

from deker.arrays import Array
from deker.client import Client
from deker.collection import Collection
from deker.ctx import CTX
from deker.errors import (
    DekerCancelledError,
    DekerClientError,
    DekerCollectionAlreadyExistsError,
    DekerCollectionNotExistsError,
    DekerCriticalError,
    DekerIntegrityError,
    DekerInvalidSchemaError,
    DekerMemoryError,
    DekerMetaDataError,
    DekerValidationError,
)
from deker.log import format_string
from deker.schemas import ArraySchema, DimensionSchema
from deker.tools import get_symlink_path
from deker.types import IntegrityCheck, IntegrityFindingKind, IntegrityReport, LocksExtensions
from deker.types.private.enums import LocksTypes
from deker.warnings import DekerWarning


class MoveAwayOnSecondRead:
    """Stand for ``read`` of the collection adapter: the second call moves the collection file away first, as a
    deletion of the collection meanwhile does; every call does the real work then.

    :param method: the real method
    :param target: where the file is moved to
    """

    def __init__(self, method, target: Path) -> None:
        self.method = method
        self.target = target
        self.calls = 0
        self.gone: list[Path] = []

    def __call__(self, adapter: LocalCollectionAdapter, name: str):
        self.calls += 1
        if self.calls == 2:
            file = adapter.collections_resource / name / (name + adapter.file_ext)
            file.rename(self.target / file.name)
            self.gone.append(file)
        return self.method(adapter, name)


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
            lambda client: client.validate_collection({}),
            lambda client: client.meta_version,
            lambda client: client.root_path,
            lambda client: client.calculate_storage_size(),
            lambda client: client.clear_locks(),
            lambda client: client.check_integrity(),
            lambda client: client.empty_trash(),
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

    @pytest.mark.parametrize(
        ("missing", "expected"),
        [
            ("fill_value", {"fill_value": np.iinfo(np.int8).min, "attributes": ()}),
            ("attributes", {"fill_value": -1, "attributes": ()}),
        ],
    )
    def test_collection_from_old_metadata_gets_one_missing_schema_field(
        self, client: Client, missing: str, expected: dict
    ):
        """A schema of an older version may lack a single field: only that one gets its default."""
        name = random_string()
        schema = {
            "dtype": "numpy.int8",
            "fill_value": -1,
            "attributes": [],
            "dimensions": [{"type": "generic", "name": "x", "size": 3, "labels": None, "scale": None}],
        }
        del schema[missing]
        collection = client.collection_from_dict(
            {
                "name": name,
                "metadata_version": "0.1",
                "storage_adapter": "HDF5StorageAdapter",
                "type": "array",
                "options": None,
                "schema": schema,
            }
        )
        try:
            assert collection.array_schema.fill_value == expected["fill_value"]
            assert collection.array_schema.attributes == expected["attributes"]
        finally:
            collection.delete()

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
        client = Client(str(embedded_uri(root_path)))
        assert client.is_open
        assert not client.is_closed
        client.close()
        assert client.is_closed
        assert not client.is_open

    def test_client_close_is_logged(self, root_path, deker_log: pytest.LogCaptureFixture):
        """Tests an explicit close tells about itself in the log."""
        client = Client(str(embedded_uri(root_path)), loglevel="INFO")
        client.close()
        assert "Client is closed" in deker_log.messages

    def test_client_leaves_the_logging_of_the_application_alone(self, root_path):
        """Tests a client sets the format of the Deker loggers only.

        The handlers of the other loggers and of the root logger keep their formats.
        """
        app_logger = logging.getLogger("tests.application")
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(message)s | application")
        handler.setFormatter(formatter)
        app_logger.addHandler(handler)
        root_handlers = list(logging.root.handlers)
        try:
            client = Client(str(embedded_uri(root_path)))
            client.close()
            assert handler.formatter is formatter
            assert logging.root.handlers == root_handlers
            assert all(h.formatter._fmt == format_string for h in logging.getLogger("Deker").handlers)
        finally:
            app_logger.removeHandler(handler)

    def test_forgotten_client_is_closed_quietly(self, root_path, deker_log: pytest.LogCaptureFixture):
        """Tests a client that is dropped without close() releases its context and writes nothing to the log.

        A finalizer runs at any moment, the interpreter shutdown included, when the log streams may be closed.
        """
        client = Client(str(embedded_uri(root_path)), loglevel="INFO")
        ctx = client._ctx
        deker_log.clear()
        del client
        gc.collect()
        assert ctx.is_closed
        assert "Client is closed" not in deker_log.messages

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

    @pytest.mark.parametrize(
        ("cause", "message"),
        [
            (ValueError("the memory limit is wrong"), "the memory limit is wrong"),
            (OSError("the server is unreachable"), "the server is unreachable"),
        ],
    )
    def test_client_constructor_error_keeps_the_message_and_the_cause(self, root_path, cause, message):
        """Tests a failure of the constructor is answered with a DekerClientError carrying one message and its cause."""
        with (
            patch.object(Client, "_open", side_effect=cause),
            pytest.raises(DekerClientError, match=message) as info,
        ):
            Client(str(embedded_uri(root_path)))
        assert type(info.value) is DekerClientError
        assert str(info.value) == message
        assert info.value.__cause__ is cause

    def test_client_constructor_passes_its_own_error_through(self, root_path):
        """Tests a DekerClientError raised while opening is not wrapped into another one."""
        error = DekerClientError("No installed adapters are found")
        with patch.object(Client, "_open", side_effect=error), pytest.raises(DekerClientError) as info:
            Client(str(embedded_uri(root_path)))
        assert info.value is error

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
    def test_client_iterator(self, params: dict, tmp_path: Path):
        """Tests client iterator on a storage of its own.

        :param params: array_collection params
        :param tmp_path: directory of the storage
        """
        with Client(embedded_uri(tmp_path)) as client:
            inserted_collection = client.create_collection(**params)
            collections = [*client]
        assert len(collections) == 1
        assert collections[0].name == inserted_collection.name

    def test_client_iterator_empty(self, tmp_path: Path):
        """Test iterator over a storage of its own without collections.

        :param tmp_path: directory of the storage
        """
        with Client(embedded_uri(tmp_path)) as client:
            assert list(client) == []

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

    def test_client_iter_skips_a_collection_gone_meanwhile(
        self, client: Client, array_schema: ArraySchema, tmp_path: Path, mocker
    ):
        """A collection whose file disappears while the client is walked is skipped; the others are yielded."""
        names = {random_string() for _ in range(3)}
        for name in names:
            client.create_collection(name, array_schema)
        read = MoveAwayOnSecondRead(LocalCollectionAdapter.read, tmp_path)
        mocker.patch.object(LocalCollectionAdapter, "read", autospec=True, side_effect=read)
        try:
            walked = {collection.name for collection in client}
            assert read.calls >= 3
            assert len(read.gone) == 1
            gone = read.gone[0].stem
            assert gone not in walked
            assert names - {gone} <= walked
        finally:
            for file in read.gone:
                (tmp_path / file.name).rename(file)
            for name in names:
                client.get_collection(name).delete()

    def test_client_check_integrity_ok(
        self, client: Client, array_schema: ArraySchema, deker_log: pytest.LogCaptureFixture
    ) -> None:
        """Tests if method returns an empty report by default and reports no errors to the log."""
        collections = [client.create_collection(i, array_schema) for i in ["test_col1", "test_col2"]]
        try:
            [col.create() for _ in range(20) for col in collections]
            report = client.check_integrity()
            assert isinstance(report, IntegrityReport)
            assert report.ok
            assert "Integrity check is running..." in deker_log.messages
            assert "Integrity check found no errors" in deker_log.messages
        finally:
            for col in collections:
                col.delete()

    @pytest.fixture
    def unlinked_array(self, array_collection_with_attributes: Collection) -> Iterator[Array]:
        """An array whose symlink is removed; the array and its collection are deleted afterwards."""
        array = array_collection_with_attributes.create(
            primary_attributes={"primary_attribute": 11},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )
        symlink_path = get_symlink_path(
            path_to_symlink_dir=array._Array__collection.path / array._adapter.symlinks_dir,
            primary_attributes_schema=array._Array__collection.array_schema.primary_attributes,
            primary_attributes=array.primary_attributes,
        )
        Path.unlink(symlink_path / os.listdir(symlink_path)[0])
        yield array
        array.delete()

    @pytest.mark.parametrize("as_type", [Path, str], ids=["path", "string"])
    def test_client_check_integrity_logfile(
        self, client: Client, unlinked_array: Array, tmp_path: Path, as_type: type
    ) -> None:
        """Tests if method writes the report to a file given as a Path or as a string."""
        filename = f"deker_integrity_report_{datetime.now().isoformat(timespec='seconds')}.txt"
        path = tmp_path / filename
        report = client.check_integrity(checks=[IntegrityCheck.links], to_file=as_type(path))
        assert [finding.kind for finding in report.findings] == [IntegrityFindingKind.symlink_missing]
        assert report.findings[0].array_id == unlinked_array.id
        with open(path) as f:
            assert f.read() == str(report)
        assert str(report).startswith(f"Collection {unlinked_array.collection}, links:\n\t- Symlink ")

    def test_client_check_integrity_without_a_file_only_logs(
        self,
        client: Client,
        unlinked_array: Array,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        deker_log: pytest.LogCaptureFixture,
    ) -> None:
        """Tests if ``to_file=False`` logs the report and writes it nowhere."""
        monkeypatch.chdir(tmp_path)
        report = client.check_integrity(checks=[IntegrityCheck.links], to_file=False)
        assert not list(tmp_path.glob("deker_integrity_report_*.txt"))
        errors = [record for record in deker_log.records if record.levelno == logging.ERROR]
        assert len(errors) == 1
        assert errors[0].getMessage() == str(report)

    def test_client_check_integrity_default_logfile(
        self,
        client: Client,
        unlinked_array: Array,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        deker_log: pytest.LogCaptureFixture,
    ) -> None:
        """Tests if ``to_file=True`` writes a default-named report to the current directory and logs it."""
        monkeypatch.chdir(tmp_path)
        report = client.check_integrity(checks=[IntegrityCheck.links], to_file=True)
        reports = list(tmp_path.glob("deker_integrity_report_*.txt"))
        assert len(reports) == 1
        assert reports[0].read_text() == str(report)
        errors = [record for record in deker_log.records if record.levelno == logging.ERROR]
        assert len(errors) == 1
        assert errors[0].getMessage() == f"{report!s}\n\nIntegrity check logged errors in {reports[0].absolute()}"

    def test_client_check_integrity_stop_on_error(self, client: Client, unlinked_array: Array) -> None:
        """Tests if ``stop_on_error`` raises the report of the first finding instead of returning it."""
        with pytest.raises(DekerIntegrityError) as info:
            client.check_integrity(checks=[IntegrityCheck.links], stop_on_error=True)
        assert "Symlink" in str(info.value)

    def test_client_check_integrity_repair(self, client: Client, unlinked_array: Array) -> None:
        """Tests if ``repair`` fixes the findings and reports them as repaired."""
        report = client.check_integrity(unlinked_array.collection, checks=[IntegrityCheck.links], repair=True)
        assert [finding.repaired for finding in report.findings] == [True]
        assert client.check_integrity(unlinked_array.collection).ok

    def test_client_check_integrity_workers(self, client: Client, unlinked_array: Array) -> None:
        """Tests if the check runs with several threads."""
        report = client.check_integrity(unlinked_array.collection, workers=4)
        assert [finding.kind for finding in report.findings] == [IntegrityFindingKind.symlink_missing]

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_client_check_integrity_reports_an_empty_collection_file(
        self, client: Client, params: dict, collection_adapter: LocalCollectionAdapter
    ) -> None:
        """Tests if an empty collection file is a finding, not an exception."""
        collection = client.create_collection(**params)
        filename = collection.path / (collection.name + collection_adapter.file_ext)
        open(filename, "w").close()
        try:
            report = client.check_integrity()
            assert [finding.kind for finding in report.findings] == [IntegrityFindingKind.collection_unreadable]
            assert report.findings[0].collection == collection.name
        finally:
            collection.delete()

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_client_check_integrity_collection(
        self,
        client: Client,
        params: dict,
        array_schema: ArraySchema,
        collection_adapter: LocalCollectionAdapter,
    ) -> None:
        """Tests if only the named collection is checked."""
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
            report = client.check_integrity(collection_1.name)
            assert [finding.collection for finding in report.findings] == [collection_1.name]
            assert report.findings[0].kind == IntegrityFindingKind.collection_unreadable
            assert "'test'" in report.findings[0].message
        finally:
            collection_1.delete()
            collection_2.delete()

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_client_check_integrity_raises_on_init(
        self, client: Client, params: dict, collection_adapter: LocalCollectionAdapter
    ) -> None:
        """Tests if method raises exception on collection initialization when told to stop on the first error."""
        collection = client.create_collection(**params)
        filename = collection.path / (collection.name + collection_adapter.file_ext)
        with open(filename, "r+") as f:
            data = json.load(f)
            data["schema"]["dtype"] = "test"
            f.seek(0)
            json.dump(data, f, indent=4)
            f.truncate()
        try:
            with pytest.raises(DekerIntegrityError):
                client.check_integrity(stop_on_error=True)
        finally:
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


class TestValidateCollection:
    """``Client.validate_collection`` checks a metadata dictionary and builds a ``Collection`` without storing it."""

    DUPLICATE_ATTRIBUTES = [
        {"name": "a", "dtype": "int", "primary": True},
        {"name": "a", "dtype": "int", "primary": False},
    ]
    DUPLICATE_DIMENSIONS = [{"type": "generic", "name": "x", "size": 3}, {"type": "generic", "name": "x", "size": 2}]
    UNSUPPORTED = "Collection metadata version is not supported by this version: adapter metadata version: 0.2, "

    @staticmethod
    def metadata(kind: str, name: str, version: str = "0.2") -> dict:
        """Build the smallest valid metadata dictionary: every optional field left out.

        :param kind: ``array`` or ``varray``
        :param name: name of the collection
        :param version: version of the metadata
        :returns: the dictionary
        """
        schema: dict = {"dtype": "numpy.int8", "dimensions": [{"type": "generic", "name": "x", "size": 3}]}
        if kind == "varray":
            schema["vgrid"] = [1]
        return {"name": name, "metadata_version": version, "type": kind, "schema": schema}

    @staticmethod
    def changed(data: dict, path: tuple, value: Any) -> dict:
        """Change one field of the dictionary in place.

        :param data: the dictionary
        :param path: keys to the field
        :param value: the new value; ``Sentinel`` - remove the field
        :returns: the same dictionary
        """
        target = data
        for key in path[:-1]:
            target = target[key]
        if value is Sentinel:
            del target[path[-1]]
        else:
            target[path[-1]] = value
        return data

    @staticmethod
    def storage(root_path: Path) -> list[str]:
        """List every path of the storage.

        :param root_path: root of the storage
        :returns: the sorted relative paths
        """
        return sorted(str(path.relative_to(root_path)) for path in root_path.rglob("*"))

    @pytest.mark.parametrize("kind", ["array", "varray"])
    def test_valid_metadata_gives_a_collection_and_stores_nothing(self, client: Client, root_path: Path, kind: str):
        """The same dictionary passes again and then creates the collection with ``collection_from_dict``."""
        name = random_string()
        data = self.metadata(kind, name)
        schema = {
            "dimensions": ({"name": "x", "size": 3, "labels": None, "scale": None, "type": "generic"},),
            "dtype": "numpy.int8",
            "attributes": (),
            "fill_value": "-128",
        }
        if kind == "varray":
            schema["vgrid"] = (1,)
        expected = {
            "name": name,
            "type": kind,
            "schema": schema,
            "options": None,
            "storage_adapter": "HDF5StorageAdapter",
            "metadata_version": "0.2",
        }
        stored_before = self.storage(root_path)

        collection = client.validate_collection(data)
        assert type(collection) is Collection
        assert collection.as_dict == expected
        assert client.get_collection(name) is None
        assert self.storage(root_path) == stored_before

        assert client.validate_collection(data).as_dict == expected
        assert client.get_collection(name) is None
        assert self.storage(root_path) == stored_before

        created = client.collection_from_dict(data)
        try:
            assert created.as_dict == expected
            stored = client.get_collection(name)
            assert stored is not None
            assert stored.as_dict == expected
            assert self.storage(root_path) != stored_before
        finally:
            created.delete()

    @pytest.mark.parametrize("version", ["0.1", "0.2"])
    @pytest.mark.parametrize("kind", ["array", "varray"])
    def test_metadata_without_the_optional_fields_gets_the_defaults_in_place(
        self, client: Client, kind: str, version: str
    ):
        name = random_string()
        data = self.metadata(kind, name, version)
        expected = self.metadata(kind, name)
        expected["schema"]["dimensions"][0].update(labels=None, scale=None)
        expected["schema"].update(fill_value=None, attributes=[])
        expected["options"] = None
        collection = client.validate_collection(data)
        assert data == expected
        assert collection.as_dict["metadata_version"] == "0.2"
        assert collection.as_dict["options"] is None

    @pytest.mark.parametrize(
        ("kind", "path", "value", "error", "message"),
        [
            pytest.param(
                "array",
                ("metadata_version",),
                Sentinel,
                DekerMetaDataError,
                UNSUPPORTED + "collection metadata version: None",
                id="no_version",
            ),
            pytest.param(
                "array",
                ("metadata_version",),
                "99.0",
                DekerMetaDataError,
                UNSUPPORTED + "collection metadata version: 99.0",
                id="newer_version",
            ),
            pytest.param(
                "array",
                ("schema", "attributes"),
                DUPLICATE_ATTRIBUTES,
                DekerValidationError,
                "Attribute name shall be unique",
                id="array_duplicate_attribute",
            ),
            pytest.param(
                "varray",
                ("schema", "attributes"),
                DUPLICATE_ATTRIBUTES,
                DekerValidationError,
                "Attribute name shall be unique",
                id="varray_duplicate_attribute",
            ),
            pytest.param(
                "array",
                ("schema", "dimensions"),
                DUPLICATE_DIMENSIONS,
                DekerValidationError,
                "Dimensions shall have unique names",
                id="array_duplicate_dimension",
            ),
            pytest.param(
                "varray",
                ("schema", "dimensions"),
                DUPLICATE_DIMENSIONS,
                DekerValidationError,
                "Dimensions shall have unique names",
                id="varray_duplicate_dimension",
            ),
            pytest.param(
                "array",
                ("schema", "dimensions"),
                [{"type": "generic", "name": "x", "size": 0}],
                DekerValidationError,
                'Dimension schema "size" shall be a positive integer',
                id="zero_size",
            ),
            pytest.param(
                "varray",
                ("schema", "vgrid"),
                [2],
                DekerValidationError,
                "Dimensions shall be split by vgrid into equal parts without remainder: "
                "x size % vgrid element = 3 % 2 = 1",
                id="vgrid_with_a_remainder",
            ),
            pytest.param(
                "array",
                ("schema", "dimensions"),
                [
                    {
                        "type": "time",
                        "name": "t",
                        "size": 3,
                        "start_value": "$dt",
                        "step": {"days": 0, "seconds": 3600, "microseconds": 0},
                    }
                ],
                DekerValidationError,
                "No dt attribute with dtype `datetime.datetime` is provided",
                id="time_start_attribute_missing",
            ),
            pytest.param(
                "array",
                ("name",),
                "",
                DekerValidationError,
                "Collection name shall be a non-empty string",
                id="empty_name",
            ),
            pytest.param(
                "array",
                ("schema", "attributes"),
                [{"name": "a", "dtype": "nonsense", "primary": True}],
                DekerInvalidSchemaError,
                "Schema \"AttributeSchema\" is invalid/corrupted : 'nonsense'",
                id="attribute_dtype_unknown",
            ),
            pytest.param(
                "array",
                ("schema", "dtype"),
                "nonsense",
                DekerMetaDataError,
                "Collection \"validated\" metadata is invalid/corrupted: 'nonsense'",
                id="dtype_unknown",
            ),
            pytest.param(
                "array",
                ("storage_adapter",),
                "Nonsense",
                DekerStorageError,
                "There is no adapter for Nonsense",
                id="storage_adapter_unknown",
            ),
            pytest.param("array", ("name",), Sentinel, KeyError, "'name'", id="no_name"),
            pytest.param("array", ("type",), "matrix", KeyError, "'matrix'", id="type_unknown"),
            pytest.param("array", ("schema",), Sentinel, KeyError, "'schema'", id="no_schema"),
        ],
    )
    def test_invalid_metadata_is_rejected_and_stores_nothing(
        self, client: Client, root_path: Path, kind: str, path: tuple, value: Any, error: type, message: str
    ):
        data = self.changed(self.metadata(kind, "validated"), path, value)
        stored_before = self.storage(root_path)
        with pytest.raises(error) as info:
            client.validate_collection(data)
        assert type(info.value) is error
        assert str(info.value) == message
        assert self.storage(root_path) == stored_before

    def test_adapter_error_of_closed_client_is_translated(self, root_path):
        """The adapter closed by its context answers with its error; the client translates it without a chain."""
        client = Client(str(embedded_uri(root_path)))
        client._ctx.is_closed = True
        with pytest.raises(DekerClientError) as info:
            client.validate_collection(self.metadata("array", random_string()))
        assert type(info.value) is DekerClientError
        assert str(info.value) == "Client is closed"
        assert info.value.__suppress_context__


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
            # a stray file among the symlinks, which are walked, is not counted either
            among_symlinks = client.root_path / collection.name / "array_symlinks" / "notes.txt"
            among_symlinks.write_text("x" * 4096)
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
    """Path of a VArray write marker as Deker < 2 made it next to the main file of an Array.

    :param filepath: main file of the Array
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

    def test_client_get_locks_lists_no_collection_lock(self, client: Client, array_collection: Collection):
        """The lock of a collection is its json file, held while it is created: nothing to list after the creation."""
        assert [lock for lock in client._get_locks() if lock["Collection"] == array_collection.name] == []

    def test_client_get_locks_skips_the_lock_files_of_older_versions(
        self, client: Client, array_collection: Collection
    ):
        """A ``<collection>.lock`` file left next to the collection by an older version is not listed."""
        old = array_collection.path.parent / (array_collection.name + ".lock")
        old.touch()
        try:
            assert [lock for lock in client._get_locks() if lock["Collection"] == array_collection.name] == []
        finally:
            old.unlink()

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
            assert expected in client._get_locks(collection_name=inserted_array.collection)
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
        assert client._get_locks() == []

    def test_client_get_locks_lists_nothing_after_a_varray_write(self, client: Client, inserted_varray):
        """Tests a VArray write leaves no lock files to list: the Arrays are held in the kernel."""
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


class TestClientSettings:
    """The timeouts of the client and the settings it takes when they are not given."""

    def test_timeouts_default_to_a_minute(self, root_path):
        with Client(str(embedded_uri(root_path))) as client:
            assert client._ctx.config.read_timeout == 60
            assert client._ctx.config.write_lock_timeout == 60

    @pytest.mark.parametrize("setting", ["read_timeout", "write_lock_timeout"])
    @pytest.mark.parametrize("value", [1, 300])
    def test_timeouts_go_to_the_config(self, root_path, setting, value):
        with Client(str(embedded_uri(root_path)), **{setting: value}) as client:
            assert getattr(client._ctx.config, setting) == value

    @pytest.mark.parametrize("setting", ["read_timeout", "write_lock_timeout"])
    @pytest.mark.parametrize("value", [0, -1, 1.5, True, "10", None])
    def test_invalid_timeouts_are_rejected(self, root_path, setting, value):
        with pytest.raises(DekerValidationError, match="positive integer number of seconds"):
            Client(str(embedded_uri(root_path)), **{setting: value})

    def test_workers_not_given_are_the_cpus_and_four(self, root_path):
        with Client(str(embedded_uri(root_path))) as client:
            assert client._ctx.config.workers == cpu_count() + 4

    def test_workers_none_is_rejected(self, root_path):
        with pytest.raises(DekerClientError, match="workers"):
            Client(str(embedded_uri(root_path)), workers=None)

    def test_executor_not_given_is_made_by_the_client(self, root_path):
        with Client(str(embedded_uri(root_path))) as client:
            assert isinstance(client._ctx.executor, ThreadPoolExecutor)

    def test_executor_given_is_used(self, root_path):
        executor = ThreadPoolExecutor(2)
        try:
            with Client(str(embedded_uri(root_path)), executor=executor) as client:
                assert client._ctx.executor is executor
        finally:
            executor.shutdown()

    @pytest.mark.parametrize("executor", [None, 4, "pool"])
    def test_executor_of_another_type_is_rejected(self, root_path, executor):
        with pytest.raises(DekerValidationError, match="executor"):
            Client(str(embedded_uri(root_path)), executor=executor)


class TestCreateCollectionTimeout:
    """The timeout of the creation of a collection is the limit of the wait for the lock of the collection."""

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            pytest.param({"timeout": 5}, 5, id="given"),
            pytest.param({}, Sentinel, id="not-given"),
        ],
    )
    def test_timeout_reaches_the_adapter(
        self, client: Client, name: str, array_schema: ArraySchema, mocker, kwargs, expected
    ):
        create = mocker.patch.object(client._adapter, "create")
        client.create_collection(name, array_schema, **kwargs)
        assert create.call_args.kwargs["timeout"] == expected
        assert isinstance(create.call_args.kwargs["token"], AbstractToken)

    @pytest.mark.parametrize("timeout", [0, -1, 1.5, True, "10", None])
    def test_invalid_timeout_is_rejected_before_the_adapter(
        self, client: Client, name: str, array_schema: ArraySchema, mocker, timeout
    ):
        create = mocker.patch.object(client._adapter, "create")
        with pytest.raises(DekerValidationError):
            client.create_collection(name, array_schema, timeout=timeout)
        create.assert_not_called()

    def test_cancelled_scope_stops_before_the_adapter(
        self, client: Client, name: str, array_schema: ArraySchema, mocker
    ):
        create = mocker.patch.object(client._adapter, "create")
        token = SimpleToken()
        token.cancel()
        with cancellation_scope(token), pytest.raises(DekerCancelledError):
            client.create_collection(name, array_schema)
        create.assert_not_called()
        assert client.get_collection(name) is None


class TestCollectionNameNotGiven:
    """The name of a collection is either a string or left out: an explicit ``None`` is rejected on entry."""

    @pytest.mark.parametrize("method", ["clear_locks", "check_integrity"])
    @pytest.mark.parametrize("name", [None, 5, b"name"])
    def test_invalid_name_is_rejected(self, client: Client, method: str, name):
        with pytest.raises(DekerValidationError, match="collection name shall be a string"):
            getattr(client, method)(name)

    @pytest.mark.parametrize("method", ["clear_locks", "check_integrity"])
    def test_name_left_out_means_every_collection(self, client: Client, inserted_array: Array, method: str):
        getattr(client, method)()

    def test_check_integrity_of_one_collection(self, client: Client, inserted_array: Array):
        assert client.check_integrity(inserted_array.collection).ok


MISSING = "Storage directory {} doesn't exist"


def critical_records(log: pytest.LogCaptureFixture) -> list[str]:
    """Messages of the critical records of the Deker loggers.

    :param log: capture of the Deker loggers
    :returns: the messages
    """
    return [record.getMessage() for record in log.records if record.levelno == logging.CRITICAL]


class TestStorageDirectories:
    """The client creates the collections and the trash of the storage when it opens; nobody else creates them.

    A directory missing when it is needed is a critical error: logged as critical and raised. Every test works on
    a storage of its own.
    """

    def test_client_creates_the_collections_and_the_trash(self, tmp_path: Path):
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)):
            assert sorted(path.name for path in storage.iterdir()) == ["collections", "trash"]
            assert all(path.is_dir() and not path.is_symlink() for path in storage.iterdir())

    def test_storage_without_a_trash_gets_one_and_keeps_its_collections(self, tmp_path: Path, array_schema):
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)) as client:
            client.create_collection("old", array_schema)
        (storage / "trash").rmdir()
        with Client(embedded_uri(storage)) as client:
            assert (storage / "trash").is_dir()
            assert [collection.name for collection in client] == ["old"]

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(lambda client, schema: client.get_collection("absent"), id="get_collection"),
            pytest.param(lambda client, schema: client.create_collection("new", schema), id="create_collection"),
            pytest.param(lambda client, schema: list(client), id="iteration"),
            pytest.param(lambda client, schema: client.calculate_storage_size(), id="calculate_storage_size"),
            pytest.param(lambda client, schema: client._get_locks(), id="locks"),
            pytest.param(lambda client, schema: client.check_integrity(), id="check_integrity"),
        ],
    )
    def test_missing_collections_directory_is_critical_and_not_created(
        self, tmp_path: Path, array_schema, deker_log: pytest.LogCaptureFixture, call
    ):
        storage = tmp_path / "storage"
        collections = storage / "collections"
        with Client(embedded_uri(storage)) as client:
            collections.rmdir()
            with pytest.raises(DekerCriticalError) as error:
                call(client, array_schema)
        assert str(error.value) == MISSING.format(collections)
        assert critical_records(deker_log) == [MISSING.format(collections)]
        assert not collections.exists()

    @pytest.mark.parametrize(
        ("module", "call"),
        [
            pytest.param("deker.client", lambda client: client._get_locks(), id="locks"),
            pytest.param("deker.integrity", lambda client: client.check_integrity(), id="check_integrity"),
        ],
    )
    def test_listing_error_with_the_collections_directory_in_place_is_raised_as_it_is(
        self, tmp_path: Path, mocker, module: str, call
    ):
        """Only a missing directory is critical: the error of the listing is not hidden otherwise."""
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)) as client:
            (storage / "collections").rmdir()
            mocker.patch(f"{module}.check_storage_directory")  # as if the directory were there
            with pytest.raises(FileNotFoundError):
                call(client)

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(lambda collection: collection.delete(), id="delete"),
            pytest.param(lambda collection: collection.clear(), id="clear"),
            pytest.param(lambda collection: list(collection), id="iteration"),
            pytest.param(lambda collection: collection.create(), id="create_array"),
        ],
    )
    def test_collection_in_a_storage_without_collections_directory_is_critical(
        self, tmp_path: Path, array_schema, deker_log: pytest.LogCaptureFixture, call
    ):
        storage = tmp_path / "storage"
        collections = storage / "collections"
        with Client(embedded_uri(storage)) as client:
            collection = client.create_collection("gone", array_schema)
            shutil.rmtree(collections)
            with pytest.raises(DekerCriticalError) as error:
                call(collection)
        assert str(error.value) == MISSING.format(collections)
        assert critical_records(deker_log) == [MISSING.format(collections)]
        assert not collections.exists()


class TestEmptyTrash:
    """``Client.empty_trash`` removes everything that lies in the trash of the storage; the trash itself stays."""

    @staticmethod
    def fill(trash: Path, outside: Path) -> None:
        """Put into the trash what operations leave there: files, directories, symlinks to files and directories.

        :param trash: the trash
        :param outside: a directory out of the trash, with the targets of the symlinks
        """
        (outside / "target-dir").mkdir()
        (outside / "target-dir" / "kept.txt").write_text("kept")
        (outside / "target.txt").write_text("kept")
        (trash / uuid4().hex).write_text("file")
        nested = trash / uuid4().hex / "array_data" / "a" / "b"
        nested.mkdir(parents=True)
        (nested / "array.hdf5").write_text("file")
        (nested.parent / "other.hdf5").write_text("file")
        (trash / uuid4().hex).mkdir()
        (trash / uuid4().hex).symlink_to(outside / "target.txt")
        (trash / uuid4().hex).symlink_to(outside / "target-dir")
        (nested.parent / "link").symlink_to(outside / "target-dir")

    def test_everything_is_removed_the_trash_and_the_targets_of_symlinks_stay(self, tmp_path: Path):
        storage = tmp_path / "storage"
        outside = tmp_path / "outside"
        outside.mkdir()
        with Client(embedded_uri(storage)) as client:
            self.fill(storage / "trash", outside)
            assert client.empty_trash() is None
            assert list((storage / "trash").iterdir()) == []
        assert (outside / "target.txt").read_text() == "kept"
        assert (outside / "target-dir" / "kept.txt").read_text() == "kept"

    def test_trash_is_emptied_before_the_call_returns_by_default(self, tmp_path: Path, mocker):
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)) as client:
            spy = mocker.spy(client._adapter, "empty_trash")
            (storage / "trash" / "left-by-an-operation").write_text("file")
            client.empty_trash()
            assert list((storage / "trash").iterdir()) == []
        spy.assert_called_once_with(background=False)

    @pytest.mark.parametrize(
        ("call", "accepted"),
        [
            pytest.param(lambda client: client.empty_trash(background=False), True, id="background-by-name"),
            pytest.param(lambda client: client.empty_trash(False), False, id="background-by-position"),
            pytest.param(lambda client: client._get_locks(collection_name=Sentinel), True, id="locks-by-name"),
            pytest.param(lambda client: client._get_locks(lock_type=LocksTypes.varray_lock), False, id="locks-by-type"),
        ],
    )
    def test_arguments_of_the_calls(self, tmp_path: Path, call, accepted: bool):
        """``background`` is given by name only, as everywhere else; the lock listing takes no type of lock."""
        with Client(embedded_uri(tmp_path / "storage")) as client:
            if accepted:
                assert call(client) in (None, [])
            else:
                with pytest.raises(TypeError):
                    call(client)

    def test_background_is_passed_to_the_adapter(self, tmp_path: Path, mocker):
        storage = tmp_path / "storage"
        with Client(embedded_uri(storage)) as client:
            emptying = mocker.patch.object(client._adapter, "empty_trash")
            client.empty_trash(background=True)
        emptying.assert_called_once_with(background=True)

    def test_emptying_does_not_obey_a_cancelled_scope(self, tmp_path: Path):
        storage = tmp_path / "storage"
        token = SimpleToken()
        token.cancel()
        with Client(embedded_uri(storage)) as client:
            (storage / "trash" / "left-by-an-operation").write_text("file")
            with cancellation_scope(token):
                client.empty_trash()
            assert list((storage / "trash").iterdir()) == []

    def test_missing_trash_is_critical_and_not_created(self, tmp_path: Path, deker_log: pytest.LogCaptureFixture):
        storage = tmp_path / "storage"
        trash = storage / "trash"
        with Client(embedded_uri(storage)) as client:
            trash.rmdir()
            with pytest.raises(DekerCriticalError) as error:
                client.empty_trash()
        assert str(error.value) == MISSING.format(trash)
        assert critical_records(deker_log) == [MISSING.format(trash)]
        assert not trash.exists()


class TestLockFilesOfOlderVersions:
    """The ``<collection>.lock`` files older versions made next to the collections stay where they are and bother
    nobody: they are not collections, not findings, not lock files of the listing, and not in the way of a creation.
    """

    def test_old_lock_files_bother_nobody(self, tmp_path: Path):
        with Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR") as own:
            schema = ArraySchema(dimensions=[DimensionSchema(name="x", size=4)], dtype=float)
            kept = own.create_collection("kept", schema)
            kept.create()[:].update(1.0)
            deleted = own.create_collection("deleted", schema)
            size = own.calculate_storage_size()
            collections = own._adapter.collections_resource
            for name in ("kept", "deleted", "orphan"):
                (collections / f"{name}.lock").touch()
            assert sorted(collection.name for collection in own) == ["deleted", "kept"]
            assert own.check_integrity().ok
            assert own.calculate_storage_size() == size
            assert own._get_locks() == []
            orphan = own.create_collection("orphan", schema)
            assert own.get_collection("orphan").as_dict == orphan.as_dict
            deleted.delete()
            assert sorted(path.name for path in collections.iterdir()) == [
                "deleted.lock",
                "kept",
                "kept.lock",
                "orphan",
                "orphan.lock",
            ]
