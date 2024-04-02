import collections

from collections import OrderedDict, namedtuple
from datetime import datetime
from typing import FrozenSet

import numpy as np
import pytest

from deker_local_adapters import LocalArrayAdapter

from deker import Client, DimensionSchema
from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerCollectionAlreadyExistsError, DekerValidationError
from deker.schemas import ArraySchema, AttributeSchema
from deker.types.private.typings import NumericDtypes


class TestAttributesSchemaNameValidation:
    """Name validation."""

    @pytest.mark.parametrize(
        "name",
        [
            1,
            0,
            -1,
            0.1,
            -0.1,
            complex(0.0000000000001),
            complex(-0.0000000000001),
            set(),
            set("abc"),
            {"abc", "def"},
            tuple(),
            tuple("abc"),
            tuple({"abc", "def"}),
            (1,),
            [],
            ["abc"],
            ["abc", "def"],
            [1],
            {},
            {"a": "b"},
            {0: 1},
            {"0": 1},
            {0: "1"},
            None,
            False,
            True,
        ],
    )
    def test_attributes_schema_name_raises_type(self, name):
        with pytest.raises(DekerValidationError):
            assert AttributeSchema(name=name, dtype=float, primary=True)

    @pytest.mark.parametrize(
        "name",
        [
            "",
            " ",
            "                  ",
        ],
    )
    def test_attributes_schema_name_raises_empty_str(self, name):
        with pytest.raises(DekerValidationError):
            assert AttributeSchema(name=name, dtype=float, primary=True)

    @pytest.mark.parametrize(
        "name",
        [
            "1",
            "0",
            "-1",
            "0.1",
            "-0.1",
            str(set()),
            'set("abc")',
            repr({"abc", "def"}),
            str(tuple()),
            'tuple("abc")',
            repr(tuple({"abc", "def"})),
            "(1,)",
            str([]),
            '["abc"]',
            repr(["abc", "def"]),
            "[1]",
            "{}",
            '{"a": "b"}',
            "{0: 1}",
            '{"0": 1}',
            '{0: "1"}',
        ],
    )
    def test_attributes_schema_name_ok(self, name):
        a = AttributeSchema(name=name, dtype=float, primary=True)
        assert a
        assert a.name == name


class TestAttributesSchemaDtypeValidation:
    @pytest.mark.parametrize("dtype", NumericDtypes + [str, tuple, datetime])
    def test_attributes_schema_dtype_ok(self, dtype):
        a = AttributeSchema(name="some_attr", dtype=dtype, primary=True)
        assert a
        assert a.dtype == dtype

    @pytest.mark.parametrize(
        "dtype",
        [
            list,
            dict,
            set,
            OrderedDict,
            FrozenSet,
            namedtuple,
            collections.deque,
            collections.Counter,
            AttributeSchema,
            ArraySchema,
            Dimension,
            Collection,
            TimeDimension,
            Array,
            VArray,
        ],
    )
    def test_attributes_schema_dtype_raises_on_type(self, dtype):
        """Test if attributes schema raises error on incorrect dtype type."""
        with pytest.raises(DekerValidationError):
            assert AttributeSchema(name="some_attr", dtype=dtype, primary=True)


class TestAttributesSchemaPrimaryValidation:
    @pytest.mark.parametrize(
        "primary",
        [
            1,
            0,
            -1,
            0.1,
            -0.1,
            complex(0.0000000000001),
            complex(-0.0000000000001),
            set(),
            set("abc"),
            {"abc", "def"},
            tuple(),
            tuple("abc"),
            tuple({"abc", "def"}),
            (1,),
            [],
            ["abc"],
            ["abc", "def"],
            [1],
            {},
            {"a": "b"},
            {0: 1},
            {"0": 1},
            {0: "1"},
            None,
        ],
    )
    def test_attributes_schema_primary_raises_type(self, primary):
        with pytest.raises(DekerValidationError):
            assert AttributeSchema(name="some_attr", dtype=float, primary=primary)

    @pytest.mark.parametrize("primary", [True, False])
    def test_attributes_schema_primary_param_ok(self, primary):
        a = AttributeSchema(name="some_attr", dtype=int, primary=primary)
        assert a
        assert a.primary == primary

    @pytest.mark.parametrize("primary", [False, True])
    @pytest.mark.parametrize(
        ("dtype", "value"),
        [
            (str, "123"),
            (str, str(complex(-125.000000000001 - 0.123456789j))),
            (str, "-125.000000000001-0.123456789j"),
            (np.int8, np.int8(1)),
            (np.int16, np.int16(-130)),
            (np.int32, np.int32(-9999)),
            (np.int64, np.int64(99999999)),
            (int, 1),
            (int, 0),
            (int, -1),
            (float, 0.1),
            (float, -0.1),
            (np.float16, np.float16(1.0)),
            (np.float32, np.float32(-130)),
            (np.float64, np.float64(-9999)),
            (np.float128, np.float128(99999999)),
            (complex, complex(0.0000000000001)),
            (complex, complex(-0.0000000000001)),
            (np.complex64, np.complex64(1.0)),
            (np.complex128, np.complex128(-130)),
            (np.complex256, np.complex256(-9999)),
            (tuple, tuple("abc")),
            (tuple, tuple({"abc", "def"})),
            (tuple, (1, 2, 3, 4)),
            (tuple, (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1)),
            (
                tuple,
                (
                    (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1),
                    (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1),
                ),
            ),
            (
                tuple,
                (
                    (
                        1,
                        0.1,
                        complex(0.0000000000001),
                        complex(-0.0000000000001),
                        -0.1,
                        -1,
                    ),
                    (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1),
                ),
            ),
            (
                tuple,
                (
                    np.int8(1),
                    np.int16(-130),
                    np.int32(-9999),
                    np.int64(99999999),
                    np.float16(1.0),
                    np.float32(-130),
                    np.float64(-9999),
                    np.float128(99999999),
                    np.complex64(1.0),
                    np.complex128(-130),
                    np.complex256(-9999),
                ),
            ),
        ],
    )
    def test_attributes_values_serialize_deserialize_ok(self, client, primary, dtype, value):
        schema = ArraySchema(
            dimensions=[DimensionSchema(name="x", size=1)],
            dtype=int,
            attributes=[AttributeSchema(name="some_attr", dtype=dtype, primary=primary)],
        )
        col_name = "test_attrs_values_validation"
        try:
            col = client.create_collection(col_name, schema)
        except DekerCollectionAlreadyExistsError:
            col = client.get_collection(col_name)
            col.clear()
        try:
            if primary:
                key = "primary_attributes"
            else:
                key = "custom_attributes"
            attrs = {key: {schema.attributes[0].name: value}}
            array = col.create(**attrs)
            assert array
            attr = getattr(array, key)
            assert attr[schema.attributes[0].name] == value
        except Exception:
            raise
        finally:
            col.delete()

    @pytest.mark.parametrize("primary", [False, True])
    @pytest.mark.parametrize(
        ("dtype", "value"),
        [
            (tuple, set("abc")),
            (tuple, list({"abc", "def"})),
            (tuple, ({1: 2}, {3: 4})),
            (tuple, ({1, 2}, {3, 4})),
            (tuple, ([1, 2], [3, 4])),
            (tuple, ([1, 2], [3, 4])),
        ],
    )
    def test_attributes_schema_raise_on_tuples_values(self, client, primary, dtype, value):
        schema = ArraySchema(
            dimensions=[DimensionSchema(name="x", size=1)],
            dtype=int,
            attributes=[AttributeSchema(name="some_attr", dtype=dtype, primary=primary)],
        )
        col_name = "test_attrs_values_validation"
        try:
            col = client.create_collection(col_name, schema)
        except DekerCollectionAlreadyExistsError:
            col = client.get_collection(col_name)
            col.clear()

        try:
            if primary:
                key = "primary_attributes"
            else:
                key = "custom_attributes"

            attrs = {key: {"some_attr": value}}
            with pytest.raises(DekerValidationError):
                assert col.create(**attrs)

        finally:
            col.delete()

    @pytest.mark.parametrize("primary", [False, True])
    @pytest.mark.parametrize(
        "dtype",
        [set, dict, None, list],
    )
    def test_attributes_schema_raises_on_dtype(self, primary, dtype):
        with pytest.raises(DekerValidationError):
            AttributeSchema(name="some_attr", dtype=dtype, primary=primary)


@pytest.mark.parametrize("primary", (True, False))
def test_schema_conversion(
    primary: bool, client: Client, local_array_adapter: LocalArrayAdapter, name: str
):
    key = "primary_attributes" if primary else "custom_attributes"

    schema = ArraySchema(
        dimensions=[DimensionSchema(name="x", size=2)],
        attributes=[
            AttributeSchema(name="units", dtype=tuple, primary=primary),
            AttributeSchema(name="dt", dtype=datetime, primary=primary),
        ],
        dtype=int,
    )
    col = client.create_collection(name, schema)
    params = {key: {"units": ("km", "C"), "dt": datetime.utcnow()}}
    array = col.create(**params)
    meta = col.arrays._ArrayManager__array_adapter.read_meta(array)
    # Before convert
    assert isinstance(meta[key]["units"], list)
    assert isinstance(meta[key]["dt"], str)

    array = Array._create_from_meta(col, meta, local_array_adapter)
    assert isinstance(getattr(array, key)["units"], tuple)
    assert isinstance(getattr(array, key)["dt"], datetime)


if __name__ == "__main__":
    pytest.main()
