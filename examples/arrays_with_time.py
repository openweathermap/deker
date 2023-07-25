import asyncio

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from deker.client import Client
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema


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
            start_value="$word",
            step=timedelta(3),
        ),
        TimeDimensionSchema(
            name="old_forecast", size=129, start_value=datetime.now(timezone.utc), step=timedelta(3)
        ),
    ]

    array_schema = ArraySchema(
        dtype=float,
        dimensions=dimensions,
        attributes=[
            # AttributeSchema(name="primary_attribute", dtype=int, primary=False),
            AttributeSchema(name="word", dtype=datetime, primary=False),
        ],
    )

    array_id = None

    # get_array_adapter client
    with Client("file:///tmp/some_coll") as client:
        new_collection: Collection = client.create_collection("GFS05FORECASTS", array_schema)
        # new_collection: Collection = client.get_collection("GFS05FORECASTS")
        # try:
        # get_array_adapter array with time key attribute for TimeDimensionSchema
        array = new_collection.create(custom_attributes={"word": datetime.now(timezone.utc)})
        array_id = array.id
        # print(array_id.custom_attributes)
        # print(array_id.dimensions)
        # print(array_id.custom_attributes)
        a = array.update_custom_attributes({"word": datetime.now(ZoneInfo("Asia/Almaty"))})
        assert a
        # print(array_id.dimensions)
        # print(array_id.custom_attributes)
        # finally:
        #     new_collection.delete()

    client = Client("file:///tmp/some_coll")
    coll = client.get_collection("GFS05FORECASTS")
    array = coll.filter({"id": array_id}).first()
    print(coll.array_schema)
    print()
    print(array.dimensions)
    print(array.custom_attributes)
    coll.delete()


if __name__ == "__main__":
    asyncio.run(main())
