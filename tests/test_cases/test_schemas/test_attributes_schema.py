import collections

from collections import OrderedDict, namedtuple
from datetime import datetime
from typing import FrozenSet

import pytest

from deker_local_adapters import LocalArrayAdapter

from deker import Client, DimensionSchema
from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerValidationError
from deker.schemas import ArraySchema, AttributeSchema
from deker.tools import create_array_from_meta
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
    def test_attributes_schema_primary_ok(self, primary):
        a = AttributeSchema(name="some_attr", dtype=int, primary=primary)
        assert a
        assert a.primary == primary


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

    array = create_array_from_meta(col, meta, local_array_adapter)
    assert isinstance(getattr(array, key)["units"], tuple)
    assert isinstance(getattr(array, key)["dt"], datetime)


if __name__ == "__main__":
    pytest.main()
