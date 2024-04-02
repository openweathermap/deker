from datetime import timedelta

import pytest

from tests.parameters.common import random_string

from deker.schemas import DimensionSchema, TimeDimensionSchema


@pytest.mark.asyncio()
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
