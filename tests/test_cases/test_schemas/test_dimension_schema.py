import pytest

from tests.parameters.schemas_params import (
    DimensionSchemaCreationParams,
    TimeDimensionSchemaCreationParams,
)

from deker.client import Client
from deker.errors import DekerValidationError
from deker.schemas import ArraySchema, DimensionSchema, TimeDimensionSchema


@pytest.mark.parametrize("params", DimensionSchemaCreationParams.WRONG_params_dataclass_raises())
def test_dimension_schema(params: dict):
    """Tests schema validation."""
    with pytest.raises(DekerValidationError):
        DimensionSchema(**params)


@pytest.mark.parametrize("params", TimeDimensionSchemaCreationParams.WRONG_params_dataclass_raises())
def test_time_dimension_schema(params: dict):
    """Tests schema validation."""
    with pytest.raises(DekerValidationError):
        assert TimeDimensionSchema(**params)


# Labels a schema accepts: a non-empty list or tuple of unique strings, or of unique finite floats, one per cell
VALID_LABELS = [
    pytest.param(["a", "b"], id="list-of-strings"),
    pytest.param(("a", "b"), id="tuple-of-strings"),
    pytest.param([1.5, 2.5], id="list-of-floats"),
    pytest.param((1.5, -2.5), id="tuple-of-floats"),
    pytest.param(["0", "1"], id="digit-strings"),
    pytest.param([0.0, 1.0], id="whole-floats"),
]

# Labels a schema rejects, each for its own reason
INVALID_LABELS = [
    pytest.param([], id="empty-list"),
    pytest.param((), id="empty-tuple"),
    pytest.param({}, id="empty-dict"),
    pytest.param({"a": 0, "b": 1}, id="dict"),
    pytest.param("ab", id="string"),
    pytest.param(b"ab", id="bytes"),
    pytest.param({"a", "b"}, id="set"),
    pytest.param([1, 2], id="ints"),
    pytest.param([True, False], id="booleans"),
    pytest.param([1.5, True], id="float-and-boolean"),
    pytest.param(["a", 1], id="string-and-int"),
    pytest.param(["a", 1.5], id="string-and-float"),
    pytest.param([None, None], id="nones"),
    pytest.param([["a"], ["b"]], id="nested-lists"),
    pytest.param(["a", "a"], id="duplicate-strings"),
    pytest.param([1.5, 1.5], id="duplicate-floats"),
    pytest.param(["a"], id="fewer-than-cells"),
    pytest.param(["a", "b", "c"], id="more-than-cells"),
    pytest.param([1.5, float("nan")], id="nan"),
    pytest.param([float("inf"), 1.5], id="infinity"),
    pytest.param(["", "a"], id="empty-string"),
    pytest.param([" ", "a"], id="blank-string"),
]


class TestDimensionSchemaLabels:
    """The labels of a DimensionSchema are validated by the schema itself, as the Dimension validates them later."""

    @pytest.mark.parametrize("labels", VALID_LABELS)
    def test_valid_labels_are_kept(self, labels):
        schema = DimensionSchema(name="x", size=2, labels=labels)
        assert schema.labels == labels
        assert schema.as_dict["labels"] == labels

    @pytest.mark.parametrize("labels", INVALID_LABELS)
    def test_invalid_labels_raise(self, labels):
        with pytest.raises(DekerValidationError):
            DimensionSchema(name="x", size=2, labels=labels)

    def test_no_labels(self):
        schema = DimensionSchema(name="x", size=2)
        assert schema.labels is None

    @pytest.mark.parametrize("labels", VALID_LABELS)
    def test_schema_and_dimension_agree(self, client: Client, labels):
        """Tests whatever the schema accepts, the Dimension of an array accepts too, and the labels come back."""
        schema = ArraySchema(dtype=float, dimensions=[DimensionSchema(name="x", size=2, labels=labels)])
        collection = client.create_collection("labels_schema_and_dimension", schema)
        try:
            array = collection.create()
            assert tuple(array.dimensions[0].labels) == tuple(labels)
            assert collection.array_schema.dimensions[0].labels == labels
        finally:
            collection.delete()


if __name__ == "__main__":
    pytest.main()
