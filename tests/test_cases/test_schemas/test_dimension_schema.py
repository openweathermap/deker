import pytest

from tests.parameters.schemas_params import (
    DimensionSchemaCreationParams,
    TimeDimensionSchemaCreationParams,
)

from deker.errors import DekerValidationError
from deker.schemas import DimensionSchema, TimeDimensionSchema


@pytest.mark.parametrize("params", DimensionSchemaCreationParams.WRONG_params_dataclass_raises())
def test_dimension_schema(params: dict):
    """Tests schema validation."""
    with pytest.raises(DekerValidationError):
        DimensionSchema(**params)


@pytest.mark.parametrize(
    "params", TimeDimensionSchemaCreationParams.WRONG_params_dataclass_raises()
)
def test_time_dimension_schema(params: dict):
    """Tests schema validation."""
    with pytest.raises(DekerValidationError):
        assert TimeDimensionSchema(**params)


if __name__ == "__main__":
    pytest.main()
