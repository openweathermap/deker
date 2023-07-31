import random
import string

from datetime import datetime, timedelta, timezone
from typing import List

import pytest

from tests.parameters.common import random_string

from deker import Scale
from deker.ABC.base_schemas import BaseDimensionSchema
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)


@pytest.fixture()
def dimensions() -> List[BaseDimensionSchema]:
    """Dimensions for Array schema."""
    return [
        DimensionSchema(
            name="x",
            size=10,
            scale=Scale(
                start_value=random.uniform(-180.0, 90.0),
                step=random.uniform(-5.0, 5.0),
                # step=1.0,
                name=random.choice((random_string(), None)),
            ),
        ),
        DimensionSchema(name="layers", size=10, labels=list(string.ascii_lowercase)[:10]),
        TimeDimensionSchema(
            name="forecast_dt",
            size=10,
            start_value=datetime(2022, 9, 1, 0, 0, tzinfo=timezone.utc),
            step=timedelta(hours=1),
        ),
    ]


@pytest.fixture()
def dimensions_with_string_time() -> List[BaseDimensionSchema]:
    """Dimensions for Array schema."""
    return [
        DimensionSchema(name="x", size=10),
        DimensionSchema(name="layers", size=10, labels=list(string.ascii_lowercase)[:10]),
        TimeDimensionSchema(
            name="forecast_dt",
            size=10,
            start_value="$time_attr_name",
            step=timedelta(hours=3),
        ),
    ]


@pytest.fixture()
def array_schema(dimensions: List[DimensionSchema]) -> ArraySchema:
    """Creates a simple Array schema.

    :param dimensions: Dimensions of array schema
    """
    return ArraySchema(dtype=float, dimensions=dimensions)  # type: ignore[arg-type]


@pytest.fixture()
def array_schema_with_attributes(dimensions_with_string_time: List[DimensionSchema]) -> ArraySchema:
    """Creates Array schema with extra primary attributes.

    :param dimensions_with_string_time: Dimensions of Array schema
    """
    return ArraySchema(
        dtype=float,
        dimensions=dimensions_with_string_time,  # type: ignore[arg-type]
        attributes=[
            AttributeSchema(name="primary_attribute", dtype=int, primary=True),
            AttributeSchema(name="custom_attribute", dtype=float, primary=False),
            AttributeSchema(name="time_attr_name", dtype=datetime, primary=False),
        ],
    )


@pytest.fixture()
def varray_schema(dimensions: List[DimensionSchema]) -> VArraySchema:
    """Creates a VArray schema."""
    return VArraySchema(dimensions=dimensions, dtype=float, vgrid=[2] * len(dimensions))  # type: ignore[arg-type]


@pytest.fixture()
def varray_schema_with_attributes(
    dimensions_with_string_time: List[DimensionSchema],
) -> VArraySchema:
    """Creates VArray schema with extra primary attributes.

    :param dimensions_with_string_time: Dimensions of Array schema
    """
    return VArraySchema(
        dtype=float,
        dimensions=dimensions_with_string_time,  # type: ignore[arg-type]
        attributes=[
            AttributeSchema(name="primary_attribute", dtype=int, primary=True),
            AttributeSchema(name="custom_attribute", dtype=float, primary=False),
            AttributeSchema(name="time_attr_name", dtype=datetime, primary=False),
        ],
        vgrid=[2] * len(dimensions_with_string_time),
    )
