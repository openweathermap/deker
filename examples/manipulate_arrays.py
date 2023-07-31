import asyncio

from deker.client import Client
from deker.collection import Collection
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema


def main():
    # prepare dimensions
    dimensions = [
        DimensionSchema(name="y", size=361),
        DimensionSchema(name="x", size=720),
        DimensionSchema(
            name="layers", size=4, labels=["temp", "pressure", "dew_point", "wind_speed"]
        ),
        # TimeDimensionSchema(
        #     name="forecast_dt",
        #     size=129,
        #     start_value=datetime.get_utc(timezone.utc),
        #     step=timedelta(hours=1),
        # ),
    ]

    array_schema = ArraySchema(
        dtype=float,
        dimensions=dimensions,
        attributes=[
            AttributeSchema(name="primary_attribute", dtype=int, primary=True),
            AttributeSchema(name="cl", dtype=float, primary=False),
        ],
    )

    # create client
    with Client("file:///tmp/some_coll") as client:
        # create new array_collection and empty array
        new_collection: Collection = client.create_collection("GFS_0053_FORECASTS", array_schema)
        try:
            # for i in range(10):
            i = 2
            new_collection.create(
                custom_attributes={"cl": 0.1 * i},
                primary_attributes={"primary_attribute": i},
            )

            collection = client.get_collection("GFS_0053_FORECASTS")

            # iterate over array_collection arrays
            array = collection.filter({"primary_attribute": 2}).last()
            # # filter arrays that are part of an array with key attribute = 1
            # my_filter = {"primary_attribute": 1}
            # filtered_arrays: FilteredManager = new_collection.arrays.filter(
            #     my_filter
            # )  # a list of filtered arrays
            #
            # first_array: Array = filtered_arrays.first()
            # print(first_array.primary_attributes)

            # # filter arrays that are part of an array with custom attribute cl > 0.5
            # my_filter = {"cl": 0.5}
            # filtered_arrays: FilteredManager = new_collection.arrays.filter(
            #     my_filter
            # )  # a list of filtered arrays

            # first_array: Array = filtered_arrays.first()
            # print(first_array.custom_attributes)

        finally:
            new_collection.delete()


# def main():
#     with Client("file:///tmp/deker_server") as client:
#         # create new array_collection and empty array
#         new_collection: Collection = client.get_collection(
#             "gfs"
#         )
#         array = new_collection.filter({'id': '78e03b98-68a8-5823-8eb8-2aad64b5ea0c'}).last()
#         print(array.primary_attributes)


if __name__ == "__main__":
    asyncio.run(main())
