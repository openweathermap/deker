import string

from datetime import datetime, timedelta
from random import choices
from string import ascii_letters

import pytest

from deker_local_adapters import HDF5Options

from deker import Scale
from deker.client import Client
from deker.collection import Collection
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)


@pytest.fixture()
def array_collection(
    client: "Client",
    array_schema: "ArraySchema",
) -> Collection:
    """Returns instance of array_collection.

    :param client: Client
    :param array_schema: Array schema
    """
    coll = client.create_collection(
        name="".join(choices(ascii_letters, k=10)),
        schema=array_schema,
    )
    yield coll
    coll.delete()


@pytest.fixture()
def array_collection_with_attributes(
    client: "Client",
    array_schema_with_attributes: "ArraySchema",
) -> Collection:
    """Returns instance of array_collection_with_attributes.

    :param client: Client
    :param array_schema_with_attributes: Array schema
    """
    coll = client.create_collection(
        name="".join(choices(ascii_letters, k=10)),
        schema=array_schema_with_attributes,
    )
    yield coll
    coll.delete()


@pytest.fixture()
def varray_collection(
    client: "Client",
    varray_schema: "VArraySchema",
) -> Collection:
    """Returns instance of varray_collection.

    :param client: Client
    :param varray_schema: VArray schema
    """
    coll = client.create_collection(
        name="".join(choices(ascii_letters, k=10)), schema=varray_schema
    )
    yield coll
    coll.delete()


@pytest.fixture()
def varray_collection_with_attributes(
    client: "Client",
    varray_schema_with_attributes: "VArraySchema",
) -> Collection:
    """Returns instance of VArray collection with attributes.

    :param client: Client
    :param varray_schema_with_attributes: VArray schema
    """
    coll = client.create_collection(
        name="".join(choices(ascii_letters, k=10)), schema=varray_schema_with_attributes
    )
    yield coll
    coll.delete()


@pytest.fixture()
def collection_options() -> HDF5Options:
    """Creates an empty collection options object."""
    return HDF5Options()


@pytest.fixture()
def scaled_collection(client) -> Collection:
    """Creates Array Collection with regular scale description."""
    schema = ArraySchema(
        dimensions=[
            DimensionSchema(name="y", size=361, scale=Scale(90.0, -0.5)),
            DimensionSchema(name="x", size=720, scale=Scale(-180.0, 0.5)),
            DimensionSchema(name="layers", size=10, labels=list(string.ascii_lowercase[:10])),
        ],
        dtype=float,
    )
    collection = client.create_collection("scaled_collection", schema)
    yield collection
    collection.delete()


@pytest.fixture()
def scaled_varray_collection(client) -> Collection:
    """Creates VArray Collection with regular scale description."""
    schema = VArraySchema(
        dimensions=[
            DimensionSchema(name="y", size=361, scale=Scale(90.0, -0.5)),
            DimensionSchema(name="x", size=720, scale=Scale(-180.0, 0.5)),
            DimensionSchema(name="layers", size=10, labels=list(string.ascii_lowercase[:10])),
        ],
        dtype=float,
        vgrid=(1, 1, 2),
    )
    collection = client.create_collection("scaled_v_collection", schema)
    yield collection
    collection.delete()


@pytest.fixture()
def timed_collection(client) -> Collection:
    """Creates Array Collection with time dimensions."""
    schema = ArraySchema(
        dimensions=[
            TimeDimensionSchema(
                name="days", size=31, start_value=datetime(2023, 1, 1), step=timedelta(days=1)
            ),
            TimeDimensionSchema(
                name="hours", size=24, start_value="$time", step=timedelta(hours=1)
            ),
        ],
        attributes=[AttributeSchema(name="time", dtype=datetime, primary=True)],
        dtype=float,
    )
    collection = client.create_collection("timed_collection", schema)
    yield collection
    collection.delete()


@pytest.fixture()
def timed_varray_collection(client) -> Collection:
    """Creates VArray collection with time dimensions."""
    schema = VArraySchema(
        dimensions=[
            TimeDimensionSchema(
                name="days", size=31, start_value=datetime(2023, 1, 1), step=timedelta(days=1)
            ),
            TimeDimensionSchema(
                name="hours", size=24, start_value="$time", step=timedelta(hours=1)
            ),
        ],
        attributes=[AttributeSchema(name="time", dtype=datetime, primary=True)],
        dtype=float,
        vgrid=(1, 6),
    )
    collection = client.create_collection("timed_v_collection", schema)
    yield collection
    collection.delete()
