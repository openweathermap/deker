import json

from datetime import datetime, timedelta, timezone

from deker.client import Client
from deker.collection import Collection
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema


def main():
    # prepare dimensions
    dimensions = [
        DimensionSchema(name="y", size=361),
        DimensionSchema(name="x", size=720),
        DimensionSchema(
            name="layers", size=4, labels=["temp", "pressure", "dew_point", "wind_speed"]
        ),
        # TimeDimensionSchema(
        #     name="dt", size=23, start_value=datetime.get_utc(tz=timezone.utc), step=timedelta(hours=3)
        # ),
        TimeDimensionSchema(
            name="dt", size=23, start_value=datetime.now(timezone.utc), step=timedelta(hours=3)
        ),
    ]

    array_schema = ArraySchema(
        dtype=float,
        dimensions=dimensions,
        attributes=[AttributeSchema(name="cl", dtype=int, primary=True)],
    )

    # create client

    uri = "file:///tmp/deker_server"
    # uri = "file:///tmp/some_coll"
    try:
        with Client(uri) as client:
            # create new array_collection
            collection: Collection = client.create_collection("GFS445", array_schema)
            arr = collection.create({"cl": 2})
            arr2 = collection.create({"cl": 2})
            print(json.dumps(collection.as_dict, indent=4))

            # update array_collection name
            # new_collection.update("new_name")
            # print(f"Collection new name: {new_collection.name}")

        # re-open client to check if data still exists
        client = Client(uri)
        new_collection = client.get_collection("GFS4")

        # prepare JSON array_collection

        # clear data inside array_collection
        new_collection.clear()

        a = new_collection.as_dict

    finally:
        # remove array_collection
        collection.delete()

    client.close()


if __name__ == "__main__":
    main()
