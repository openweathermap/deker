import asyncio

from datetime import timedelta

import numpy as np

from deker.client import Client
from deker.collection import Collection
from deker.schemas import DimensionSchema, TimeDimensionSchema, VArraySchema
from deker.tools import get_utc


def main():
    # prepare dimensions
    dimensions = [
        DimensionSchema(name="x", size=10),
        DimensionSchema(name="layers", size=10),
        TimeDimensionSchema(
            name="forecast_dt", size=10, step=timedelta(hours=10), start_value=get_utc()
        ),
    ]

    array_schema = VArraySchema(
        dtype=float,
        dimensions=dimensions,
        vgrid=(2, 2, 2),
        attributes=[],
    )

    with Client("file:///tmp/some_coll") as client:
        # create new array_collection and empty array
        new_collection: Collection = client.create_collection("GFS_005_FORECASTS", array_schema)
        try:
            # for i in range(10):
            varray = new_collection.create()
            subset = varray[:]
            subset.update(data=np.zeros(shape=array_schema.shape))
            assert np.array_equal((subset.read()), np.zeros(shape=array_schema.shape))
        finally:
            new_collection.delete()


if __name__ == "__main__":
    asyncio.run(main())
