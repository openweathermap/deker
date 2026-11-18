from datetime import datetime, timedelta, timezone

import pytest

from tests.parameters.common import random_string

from deker.errors import DekerValidationError
from deker.schemas import DimensionSchema, TimeDimensionSchema
from deker.validators import process_time_dimension_attrs


@pytest.mark.asyncio
class TestNoVgridValidateAttributesTimeDimension:
    """Tests for deker.validators.process_time_dimension_attrs.

    If attribute for time dimension is a primary attributes - attributes validator shall fail.
    """

    def coll_time_params(self) -> dict:
        return {
            "name": random_string(),
            "schema": dict(
                dtype=float,
                dimensions=[
                    DimensionSchema(name="dim1", size=10, labels=None),
                    DimensionSchema(name="dim2", size=10, labels=None),
                    TimeDimensionSchema(
                        name="time_dim1",
                        size=10,
                        start_value="$start1",
                        step=timedelta(4),
                    ),
                    TimeDimensionSchema(
                        name="time_dim2",
                        size=10,
                        start_value="$start2",
                        step=timedelta(4),
                    ),
                ],
                attributes=[],
            ),
        }


if __name__ == "__main__":
    pytest.main()


class TestProcessTimeDimensionAttrs:
    """The start of a $-time dimension is taken from the attribute of that name and brought to UTC."""

    def test_missing_attribute_raises(self):
        with pytest.raises(DekerValidationError, match="No start value provided for time dimension"):
            process_time_dimension_attrs({"other": datetime(2024, 1, 1, tzinfo=timezone.utc)}, "start")

    def test_none_attribute_raises(self):
        with pytest.raises(DekerValidationError, match="No start value provided for time dimension"):
            process_time_dimension_attrs({"start": None}, "start")

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param(
                datetime(2024, 1, 1, 12), datetime(2024, 1, 1, 12, tzinfo=timezone.utc), id="naive-taken-as-utc"
            ),
            pytest.param(
                datetime(2024, 1, 1, 12, tzinfo=timezone(timedelta(hours=3))),
                datetime(2024, 1, 1, 9, tzinfo=timezone.utc),
                id="other-timezone-converted",
            ),
            pytest.param(
                datetime(2024, 1, 1, 12, tzinfo=timezone.utc), datetime(2024, 1, 1, 12, tzinfo=timezone.utc), id="utc"
            ),
        ],
    )
    def test_attribute_is_returned_in_utc(self, value: datetime, expected: datetime):
        result = process_time_dimension_attrs({"start": value}, "start")
        assert result == expected
        assert result.tzinfo == timezone.utc
