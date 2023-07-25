import asyncio

from datetime import datetime, timedelta, timezone

import numpy as np

from deker.arrays import Array
from deker.client import Client
from deker.collection import Collection
from deker.config import DekerConfig
from deker.schemas import ArraySchema, DimensionSchema, TimeDimensionSchema


def main():
    # prepare dimensions
    dimensions = [
        DimensionSchema(name="y", size=361),
        DimensionSchema(name="x", size=720),
        DimensionSchema(
            name="layers", size=4, labels=["temp", "pressure", "dew_point", "wind_speed"]
        ),
        TimeDimensionSchema(
            name="forecast_dt",
            size=129,
            start_value=datetime.now(timezone.utc),
            step=timedelta(3),
        ),
    ]

    array_schema = ArraySchema(dtype=float, dimensions=dimensions)

    uri = "file:///tmp/some_coll"
    config = DekerConfig(deker_thread_workers=1)

    # create client
    with Client(uri, config=config) as client:
        # create new array_collection and empty array
        collection: Collection = client.create_collection("eqeddwqdw", array_schema)
        empty_array: Array = collection.create()
        print(empty_array.as_dict)
        # print("Array as dict", json.dumps(empty_array.as_dict, indent=4))

        # create new array with some data
        data = np.ones(shape=collection.array_schema.shape, dtype=collection.array_schema.dtype)
        array_with_data: Array = collection.create()
        subset = array_with_data[:]
        subset.update(data)
        # print("\nArray data", subset.read())

        # update subset data
        data[0] = -999
        updated = subset.update(data)
        updated_data = subset.read()
        # print("\nUpdated data: ", updated_data)

        # clear subset data
        cleared = subset.clear()
        cleared_data = subset.read()
        # print("\nCleared data: ", cleared_data)

        # delete array
        array_deleted = empty_array.delete()
        collection_deleted = collection.delete()
        print("Finish")


if __name__ == "__main__":
    asyncio.run(main())
