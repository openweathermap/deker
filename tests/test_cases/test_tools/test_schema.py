"""Tests of the schema tools: building schemas and dimensions from stored metadata."""

import pytest

from deker.errors import DekerInvalidSchemaError
from deker.schemas import AttributeSchema
from deker.tools.schema import create_attributes_schema


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
