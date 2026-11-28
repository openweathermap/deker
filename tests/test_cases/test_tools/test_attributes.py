"""Tests of the attributes serialization tools."""

from datetime import datetime, timezone

import numpy as np
import pytest

from deker.tools.attributes import deserialize_attribute_value, serialize_attribute_value


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(np.array([1, 2, 3]), [1, 2, 3], id="int-array"),
        pytest.param(np.array([[1.5, 2.5], [3.5, 4.5]]), [[1.5, 2.5], [3.5, 4.5]], id="nested-float-array"),
        pytest.param(np.array([], dtype=np.int8), [], id="empty-array"),
        pytest.param(np.int32(7), 7, id="numpy-int"),
        pytest.param(np.float32(0.5), 0.5, id="numpy-float"),
        pytest.param(np.complex64(1 + 2j), "(1+2j)", id="numpy-complex"),
        pytest.param(1 + 2j, "(1+2j)", id="complex"),
        pytest.param(datetime(2024, 1, 1, tzinfo=timezone.utc), "2024-01-01T00:00:00+00:00", id="datetime"),
        pytest.param("text", "text", id="string"),
    ],
)
def test_serialize_attribute_value(value, expected):
    serialized = serialize_attribute_value(value)
    assert serialized == expected
    assert type(serialized) is type(expected)


class TestDeserializeStringsInTuples:
    """A string inside a tuple may be a serialized complex number; anything else stays a string."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param("(1+2j)", 1 + 2j, id="complex"),
            pytest.param("(-1.5-2.5j)", -1.5 - 2.5j, id="negative-complex"),
            pytest.param("(1e1-1j)", 10 - 1j, id="exponent"),
        ],
    )
    def test_a_serialized_complex_number_is_restored(self, value: str, expected: complex):
        restored = deserialize_attribute_value(value, str, from_tuple=True)
        assert restored == expected
        assert isinstance(restored, complex)

    @pytest.mark.parametrize("value", ["(11ej)", "(12ej)"])
    def test_a_string_that_only_looks_like_a_complex_number_stays_a_string(self, value: str):
        assert deserialize_attribute_value(value, str, from_tuple=True) == value

    @pytest.mark.parametrize("value", ["(1+2j)", "text", "1"])
    def test_a_string_outside_a_tuple_is_never_parsed(self, value: str):
        assert deserialize_attribute_value(value, str, from_tuple=False) == value
