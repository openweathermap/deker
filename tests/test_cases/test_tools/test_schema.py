"""Tests of the schema tools: building schemas and dimensions from stored metadata."""

import datetime

import numpy as np
import pytest

from tests.parameters.fresh_copy import load_fresh_copy

from deker.errors import DekerInvalidSchemaError, DekerMetaDataError
from deker.schemas import AttributeSchema
from deker.tools import schema as schema_module
from deker.tools.schema import create_attributes_schema, dtype_from_name
from deker.types.private import enums as enums_module


def test_create_attributes_schema_from_metadata():
    schemas = create_attributes_schema(
        [
            {"name": "a", "dtype": "numpy.int32", "primary": True},
            {"name": "b", "dtype": "string", "primary": False},
            {"name": "c", "dtype": "datetime", "primary": False},
        ]
    )
    assert [schema.as_dict for schema in schemas] == [
        {"name": "a", "dtype": "numpy.int32", "primary": True},
        {"name": "b", "dtype": "string", "primary": False},
        {"name": "c", "dtype": "datetime", "primary": False},
    ]
    assert all(isinstance(schema, AttributeSchema) for schema in schemas)


@pytest.mark.parametrize(
    "metadata",
    [
        pytest.param([{"name": "a", "primary": True}], id="no-dtype"),
        pytest.param([{"name": "a", "dtype": "numpy.int128", "primary": True}], id="unknown-dtype"),
        pytest.param([{"name": "a", "dtype": 5, "primary": True}], id="dtype-not-a-string"),
        pytest.param([{"dtype": "numpy.int32", "primary": True}], id="no-name"),
    ],
)
def test_create_attributes_schema_from_corrupted_metadata_raises(metadata: list):
    with pytest.raises(DekerInvalidSchemaError, match="invalid/corrupted"):
        create_attributes_schema(metadata)


class TestDtypeFromName:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("int", int),
            ("float", float),
            ("string", str),
            ("datetime", datetime.datetime),
            ("tuple", tuple),
            ("numpy.int8", np.int8),
            ("numpy.float32", np.float32),
            ("numpy.complex64", np.complex64),
        ],
    )
    def test_known_names_give_their_types(self, name: str, expected: type):
        assert dtype_from_name(name) is expected

    @pytest.mark.parametrize("name", ["numpy.int128", "whatever", ""])
    def test_unknown_names_raise_key_error(self, name: str):
        """An unknown name is corrupted metadata: the callers turn the KeyError into their own error."""
        with pytest.raises(KeyError):
            dtype_from_name(name)

    @pytest.mark.parametrize("name", ["numpy.float128", "numpy.complex256"])
    def test_platform_dependent_dtype_absent_in_numpy_raises_a_clear_error(
        self, name: str, monkeypatch: pytest.MonkeyPatch
    ):
        """Without the type in NumPy (Apple silicon, Windows) the error names the platform, not corrupted metadata."""
        monkeypatch.delattr(np, name.split("numpy.")[-1], raising=False)
        enums = load_fresh_copy(enums_module, "deker.types.private.enums_without_long_types")
        monkeypatch.setattr(schema_module, "DTypeEnum", enums.DTypeEnum)
        with pytest.raises(DekerMetaDataError, match="does not provide it on this platform"):
            dtype_from_name(name)

    @pytest.mark.parametrize(
        ("name", "expected"), [("numpy.float128", np.longdouble), ("numpy.complex256", np.clongdouble)]
    )
    def test_platform_dependent_dtype_present_in_numpy_is_returned(
        self, name: str, expected: type, monkeypatch: pytest.MonkeyPatch
    ):
        """With the type in NumPy (Linux, Intel macOS) the name maps to it like any other."""
        monkeypatch.setattr(np, name.split("numpy.")[-1], expected, raising=False)
        enums = load_fresh_copy(enums_module, "deker.types.private.enums_with_long_types")
        monkeypatch.setattr(schema_module, "DTypeEnum", enums.DTypeEnum)
        assert dtype_from_name(name) is expected


def test_create_attributes_schema_with_a_dtype_absent_on_this_platform_raises_a_clear_error(
    monkeypatch: pytest.MonkeyPatch,
):
    """The platform error passes through create_attributes_schema as it is, not as corrupted metadata."""
    monkeypatch.delattr(np, "float128", raising=False)
    enums = load_fresh_copy(enums_module, "deker.types.private.enums_without_long_types")
    monkeypatch.setattr(schema_module, "DTypeEnum", enums.DTypeEnum)
    with pytest.raises(DekerMetaDataError, match="numpy.float128 is not available"):
        create_attributes_schema([{"name": "a", "dtype": "numpy.float128", "primary": True}])
