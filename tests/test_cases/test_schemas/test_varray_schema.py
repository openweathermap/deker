from datetime import datetime, timedelta, timezone
from typing import Any, List, Union

import numpy as np
import pytest

from tests.parameters.schemas_params import VArraySchemaCreationParams

from deker.errors import DekerValidationError
from deker.schemas import DimensionSchema, TimeDimensionSchema, VArraySchema


@pytest.mark.parametrize("params", VArraySchemaCreationParams.WRONG_params_dataclass_raises())
def test_varray_schema(params: dict):
    """Tests schema validation.

    :param params: attributes schema params
    """
    with pytest.raises(DekerValidationError):
        assert VArraySchema(**params)


@pytest.mark.parametrize(
    "vgrid",
    [
        None,
        True,
        False,
        "",
        0,
        1,
        1.2,
        object,
        object(),
        [],
        set(),
        dict(),
        tuple(),
        datetime.now(),
    ],
)
def test_varray_schema_vgrid_raises_on_type(dimensions, vgrid: Any):
    with pytest.raises(DekerValidationError):
        assert VArraySchema(dtype=float, dimensions=dimensions, vgrid=vgrid)


@pytest.mark.parametrize(
    "arrays_shape",
    [
        None,
        True,
        False,
        "",
        0,
        1,
        1.2,
        object,
        object(),
        [],
        set(),
        dict(),
        tuple(),
        datetime.now(),
    ],
)
def test_varray_schema_arrays_shape_raises_on_type(dimensions, arrays_shape: Any):
    with pytest.raises(DekerValidationError):
        assert VArraySchema(dtype=float, dimensions=dimensions, arrays_shape=arrays_shape)


@pytest.mark.parametrize(
    ("splitter", "expected"),
    [
        (dict(vgrid=(1, 1, 1)), (10, 10, 10)),
        (dict(arrays_shape=(1, 1, 1)), (10, 10, 10)),
        (dict(vgrid=(2, 2, 2)), (5, 5, 5)),
        (dict(arrays_shape=(5, 5, 5)), (2, 2, 2)),
        (dict(vgrid=(5, 5, 5)), (2, 2, 2)),
        (dict(arrays_shape=(2, 2, 2)), (5, 5, 5)),
        (dict(vgrid=(1, 2, 5)), (10, 5, 2)),
        (dict(arrays_shape=(10, 5, 2)), (1, 2, 5)),
        (dict(vgrid=(5, 2, 1)), (2, 5, 10)),
        (dict(arrays_shape=(2, 5, 10)), (5, 2, 1)),
        (dict(vgrid=(2, 1, 5)), (5, 10, 2)),
        (dict(arrays_shape=(5, 10, 2)), (2, 1, 5)),
    ],
)
def test_varray_schema_is_split_correctly(dimensions, splitter: dict, expected):
    schema = VArraySchema(dtype=float, dimensions=dimensions, **splitter)
    if list(splitter.keys())[0] == "vgrid":
        assert schema.vgrid == list(splitter.values())[0]
        assert schema.arrays_shape == expected
    else:
        assert schema.vgrid == expected
        assert schema.arrays_shape == list(splitter.values())[0]


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
@pytest.mark.parametrize(
    "splitter",
    [
        dict(vgrid=(1, 1)),
        dict(arrays_shape=(1, 1)),
    ],
)
def test_dimensions_non_unique_names_error(
    dims: List[Union[DimensionSchema, TimeDimensionSchema]], splitter
):
    with pytest.raises(DekerValidationError):
        assert VArraySchema(dtype=int, dimensions=dims, **splitter)  # type: ignore[arg-type]


@pytest.mark.parametrize("vgrid_class", [tuple, list])
def test_varray_schema_vgrid_raises_on_type2(dimensions, vgrid_class: object()):  # type: ignore[valid-type]
    dims = len(dimensions)
    sum = [2] * (dims + 1)
    sub = [2] * (dims - 1)
    for s in (sum, sub):
        vgrid = vgrid_class(s)
        with pytest.raises(DekerValidationError):
            assert VArraySchema(dtype=float, dimensions=dimensions, vgrid=vgrid)


@pytest.mark.parametrize(
    "vgrid",
    [
        (0,),
        (0, 0),
        (0, 0, 0),
        (3,),
        (
            2,
            3,
        ),
        (2, 2, 3),
    ],
)
def test_varray_schema_vgrid_raises_on_value(dimensions, vgrid: tuple):
    with pytest.raises(DekerValidationError):
        assert VArraySchema(dtype=float, dimensions=dimensions, vgrid=vgrid)


@pytest.mark.parametrize(
    "arrays_shape",
    [
        (0,),
        (0, 0),
        (0, 0, 0),
        (3,),
        (
            2,
            3,
        ),
        (2, 2, 3),
    ],
)
def test_varray_schema_arrays_shape_raises_on_value(dimensions, arrays_shape: tuple):
    with pytest.raises(DekerValidationError):
        assert VArraySchema(dtype=float, dimensions=dimensions, arrays_shape=arrays_shape)


def test_varray_schema_str(varray_schema: VArraySchema):
    assert str(varray_schema) == repr(varray_schema)


def test_varray_schema_primary_attributes(varray_schema_with_attributes: VArraySchema):
    assert isinstance(varray_schema_with_attributes.primary_attributes, tuple)
    attrs = tuple(attr for attr in varray_schema_with_attributes.attributes if attr.primary)
    assert varray_schema_with_attributes.primary_attributes == attrs


def test_varray_schema_custom_attributes(varray_schema_with_attributes: VArraySchema):
    assert isinstance(varray_schema_with_attributes.custom_attributes, tuple)
    attrs = tuple(attr for attr in varray_schema_with_attributes.attributes if not attr.primary)
    assert varray_schema_with_attributes.custom_attributes == attrs


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
@pytest.mark.parametrize(
    "splitter",
    [
        dict(vgrid=(1, 1, 1)),
        dict(arrays_shape=(1, 1, 1)),
    ],
)
def test_varray_schema_fill_value(dimensions, dtype, fill_value, splitter):
    s = VArraySchema(dtype=dtype, dimensions=dimensions, fill_value=fill_value, **splitter)
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
@pytest.mark.parametrize(
    "splitter",
    [
        dict(vgrid=(1, 1, 1)),
        dict(arrays_shape=(1, 1, 1)),
    ],
)
def test_varray_schema_fill_value_raises(dimensions, dtype, fill_value, splitter):
    with pytest.raises(DekerValidationError):
        VArraySchema(dtype=dtype, dimensions=dimensions, fill_value=fill_value, **splitter)


@pytest.mark.parametrize(
    ("user_dtype", "dtype"),
    [
        (int, np.int64),
        (float, np.float64),
        (complex, np.complex128),
    ],
)
@pytest.mark.parametrize(
    "splitter",
    [
        dict(vgrid=(1, 1, 1)),
        dict(arrays_shape=(1, 1, 1)),
    ],
)
def test_varray_schema_dtype_converting(dimensions, user_dtype, dtype, splitter):
    s = VArraySchema(dtype=user_dtype, dimensions=dimensions, **splitter)
    assert s.dtype == dtype


if __name__ == "__main__":
    pytest.main()
