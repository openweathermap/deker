from datetime import datetime, timedelta

import pytest

from tests.parameters.common import random_string

from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerCollectionAlreadyExistsError, DekerValidationError
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema


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

    @pytest.mark.parametrize(
        "custom_attributes",
        [
            {},
            {"start1": None},
            {"start2": None},
            {"start1": None, "start2": None},
        ],
    )
    def test_time_custom_attributes_None(self, client: Client, custom_attributes: dict):
        """Tests errors on None custom attributes for time dimensions."""

        coll_params = self.coll_time_params()
        coll_params["schema"]["attributes"].extend(
            [
                AttributeSchema(name="start1", dtype=datetime, primary=False),
                AttributeSchema(name="start2", dtype=datetime, primary=False),
            ]
        )
        coll_params["schema"] = ArraySchema(**coll_params["schema"])  # type: ignore
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                assert collection.create(custom_attributes=custom_attributes)
        finally:
            collection.delete()


if __name__ == "__main__":
    pytest.main()
