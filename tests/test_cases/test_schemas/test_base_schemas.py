"""Tests of the schema interfaces."""

from datetime import datetime, timedelta

import pytest

from deker.errors import DekerInvalidSchemaError
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema


@pytest.mark.parametrize(
    "schema",
    [
        pytest.param(DimensionSchema(name="x", size=2), id="dimension"),
        pytest.param(
            TimeDimensionSchema(name="t", size=2, start_value=datetime(2024, 1, 1), step=timedelta(hours=1)),
            id="time-dimension",
        ),
        pytest.param(AttributeSchema(name="a", dtype=int, primary=True), id="attribute"),
        pytest.param(ArraySchema(dtype=int, dimensions=[DimensionSchema(name="x", size=2)]), id="array"),
    ],
)
def test_str_is_repr(schema):
    assert str(schema) == repr(schema)
    assert str(schema).startswith(schema.__class__.__name__)


def test_as_dict_of_an_array_schema_with_a_corrupted_dtype_raises():
    """The dtype is checked again at serialization: a schema corrupted after validation does not reach the storage."""
    schema = ArraySchema(dtype=int, dimensions=[DimensionSchema(name="x", size=2)])
    schema.dtype = str
    with pytest.raises(DekerInvalidSchemaError, match="wrong dtype"):
        schema.as_dict
