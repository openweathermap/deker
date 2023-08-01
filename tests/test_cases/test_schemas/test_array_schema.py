from datetime import datetime, timedelta, timezone
from typing import List, Union

import numpy as np
import pytest

from tests.parameters.schemas_params import ArraySchemaCreationParams

from deker.errors import DekerValidationError
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema


@pytest.mark.parametrize("params", ArraySchemaCreationParams.WRONG_params_dataclass_raises())
def test_array_schema(params: dict):
    """Tests schema validation.

    :param params: attributes schema params
    """
    with pytest.raises(DekerValidationError):
        assert ArraySchema(**params)


@pytest.mark.parametrize(
    "dims",
    [
        [DimensionSchema(name="x", size=1), DimensionSchema(name="x", size=2)],
        [
            TimeDimensionSchema(
                name="x", size=1, step=timedelta(1), start_value=datetime.now(tz=timezone.utc)
            ),
            DimensionSchema(name="x", size=2),
        ],
        [
            DimensionSchema(name="x", size=2),
            TimeDimensionSchema(
                name="x", size=1, step=timedelta(1), start_value=datetime.now(tz=timezone.utc)
            ),
        ],
    ],
)
def test_dimensions_non_unique_names_error(dims: List[Union[DimensionSchema, TimeDimensionSchema]]):
    with pytest.raises(DekerValidationError):
        assert ArraySchema(dtype=int, dimensions=dims)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "attrs",
    [
        [
            AttributeSchema(name="name", dtype=int, primary=True),
            AttributeSchema(name="name", dtype=float, primary=False),
        ],
        [
            AttributeSchema(name="name", dtype=int, primary=True),
            AttributeSchema(name="name", dtype=float, primary=True),
        ],
        [
            AttributeSchema(name="name", dtype=int, primary=False),
            AttributeSchema(name="name", dtype=float, primary=False),
        ],
    ],
)
def test_attributes_non_unique_names_error(
    attrs: List[AttributeSchema], dimensions: List[DimensionSchema]
):
    with pytest.raises(DekerValidationError):
        assert ArraySchema(dtype=int, dimensions=dimensions, attributes=attrs)  # type: ignore[arg-type]


def test_array_schema_repr(array_schema):
    assert str(array_schema) == repr(array_schema)


def test_array_schema_primary_attributes(array_schema_with_attributes):
    assert isinstance(array_schema_with_attributes.primary_attributes, tuple)
    attrs = tuple(attr for attr in array_schema_with_attributes.attributes if attr.primary)
    assert array_schema_with_attributes.primary_attributes == attrs


def test_array_schema_custom_attributes(array_schema_with_attributes):
    assert isinstance(array_schema_with_attributes.custom_attributes, tuple)
    attrs = tuple(attr for attr in array_schema_with_attributes.attributes if not attr.primary)
    assert array_schema_with_attributes.custom_attributes == attrs


@pytest.mark.parametrize(
    ("dtype", "fill_value"),
    [
        (int, -9223372036854775808),
        (float, np.nan),
        (complex, np.nan),
        (np.int8, -128),
        (np.int16, -32768),
        (np.int32, -2147483648),
        (np.int64, -9223372036854775808),
        (np.longlong, -9223372036854775808),
        (np.float16, np.nan),
        (np.float32, np.nan),
        (np.float64, np.nan),
        (np.float128, np.nan),
        (np.longfloat, np.nan),
        (np.double, np.nan),
        (np.longdouble, np.nan),
        (np.complex64, np.nan),
        (np.complex128, np.nan),
        (np.complex256, np.nan),
        (np.longcomplex, np.nan),
    ],
)
def test_array_schema_fill_value(dimensions, dtype, fill_value):
    """Test if array schema fill value is set correctly."""
    s = ArraySchema(dtype=dtype, dimensions=dimensions)
    if np.isnan(fill_value):
        assert np.isnan(s.fill_value)
    else:
        assert s.fill_value == fill_value


@pytest.mark.parametrize(
    ("dtype", "fill_value"),
    [
        (int, np.nan),
        (np.int8, np.nan),
        (np.int16, np.nan),
        (np.int32, np.nan),
        (np.int64, np.nan),
        (np.longlong, np.nan),
    ],
)
def test_array_schema_fill_value_raises(dimensions, dtype, fill_value):
    with pytest.raises(DekerValidationError):
        ArraySchema(dtype=dtype, dimensions=dimensions, fill_value=fill_value)


@pytest.mark.parametrize(
    ("user_dtype", "dtype"),
    [
        (int, np.int64),
        (float, np.float64),
        (complex, np.complex128),
    ],
)
def test_array_schema_dtype_converting(dimensions, user_dtype, dtype):
    s = ArraySchema(dtype=user_dtype, dimensions=dimensions)
    assert s.dtype == dtype


def test_array_schema_shape_is_python_int(array_schema: ArraySchema):
    shape = array_schema.shape
    for i in shape:
        assert type(i) == int


if __name__ == "__main__":
    pytest.main()
