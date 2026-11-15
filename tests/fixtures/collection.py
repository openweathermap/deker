import string

from datetime import datetime, timedelta
from random import choices
from string import ascii_letters

import numpy as np
import pytest

from deker_local_adapters import HDF5Options

from tests.parameters.uri import embedded_uri

from deker import Scale
from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)


@pytest.fixture
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


@pytest.fixture
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


@pytest.fixture
def varray_collection(
    client: "Client",
    varray_schema: "VArraySchema",
) -> Collection:
    """Returns instance of varray_collection.

    :param client: Client
    :param varray_schema: VArray schema
    """
    coll = client.create_collection(name="".join(choices(ascii_letters, k=10)), schema=varray_schema)
    yield coll
    coll.delete()


@pytest.fixture
def varray_collection_with_attributes(
    client: "Client",
    varray_schema_with_attributes: "VArraySchema",
) -> Collection:
    """Returns instance of VArray collection with attributes.

    :param client: Client
    :param varray_schema_with_attributes: VArray schema
    """
    coll = client.create_collection(name="".join(choices(ascii_letters, k=10)), schema=varray_schema_with_attributes)
    yield coll
    coll.delete()


@pytest.fixture
def collection_options() -> HDF5Options:
    """Creates an empty collection options object."""
    return HDF5Options()


@pytest.fixture
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


@pytest.fixture
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


@pytest.fixture
def timed_collection(client) -> Collection:
    """Creates Array Collection with time dimensions."""
    schema = ArraySchema(
        dimensions=[
            TimeDimensionSchema(name="days", size=31, start_value=datetime(2023, 1, 1), step=timedelta(days=1)),
            TimeDimensionSchema(name="hours", size=24, start_value="$time", step=timedelta(hours=1)),
        ],
        attributes=[AttributeSchema(name="time", dtype=datetime, primary=True)],
        dtype=float,
    )
    collection = client.create_collection("timed_collection", schema)
    yield collection
    collection.delete()


@pytest.fixture
def timed_varray_collection(client) -> Collection:
    """Creates VArray collection with time dimensions."""
    schema = VArraySchema(
        dimensions=[
            TimeDimensionSchema(name="days", size=31, start_value=datetime(2023, 1, 1), step=timedelta(days=1)),
            TimeDimensionSchema(name="hours", size=24, start_value="$time", step=timedelta(hours=1)),
        ],
        attributes=[AttributeSchema(name="time", dtype=datetime, primary=True)],
        dtype=float,
        vgrid=(1, 6),
    )
    collection = client.create_collection("timed_v_collection", schema)
    yield collection
    collection.delete()


class SteppedCollections:
    """Collections of the step tests, one per shape and grid, created on first use and deleted at the end.

    Every parametrized case asks for its own shape and grid; keeping the collections here saves creating
    the same collection for every case, whatever the order the tests run in.

    :param client: client that owns the collections
    """

    def __init__(self, client: Client) -> None:
        self.client = client
        self.collections: dict[tuple, Collection] = {}
        self.filled: dict[tuple, Array | VArray] = {}

    @staticmethod
    def _dimensions(shape: tuple[int, ...]) -> list[DimensionSchema]:
        """Plain dimensions of the given shape.

        :param shape: shape of the array
        :returns: dimension schemas
        """
        return [DimensionSchema(name=f"d{n}", size=size) for n, size in enumerate(shape)]

    def array_collection(self, shape: tuple[int, ...]) -> Collection:
        """Get the Array collection of the shape, creating it on first use.

        :param shape: shape of the arrays
        :returns: the collection
        """
        key = ("array", shape)
        if key not in self.collections:
            schema = ArraySchema(dtype=float, dimensions=self._dimensions(shape))
            name = f"stepped_array_{'_'.join(map(str, shape))}"
            self.collections[key] = self.client.create_collection(name, schema)
        return self.collections[key]

    def varray_collection(self, shape: tuple[int, ...], vgrid: tuple[int, ...]) -> Collection:
        """Get the VArray collection of the shape and the grid, creating it on first use.

        :param shape: shape of the varrays
        :param vgrid: grid of the varrays
        :returns: the collection
        """
        key = ("varray", shape, vgrid)
        if key not in self.collections:
            schema = VArraySchema(dtype=float, dimensions=self._dimensions(shape), vgrid=vgrid)
            name = f"stepped_varray_{'_'.join(map(str, shape))}_{'_'.join(map(str, vgrid))}"
            self.collections[key] = self.client.create_collection(name, schema)
        return self.collections[key]

    @staticmethod
    def data(shape: tuple[int, ...]) -> np.ndarray:
        """Distinct values for every cell of the shape: ``1..n``.

        :param shape: shape of the array
        :returns: the values
        """
        return np.arange(1, np.prod(shape, dtype=int) + 1, dtype=float).reshape(shape)

    def filled_array(self, shape: tuple[int, ...]) -> Array:
        """Get the Array of the shape holding ``data(shape)``, written on first use; it is never changed.

        :param shape: shape of the array
        :returns: the array
        """
        key = ("filled-array", shape)
        if key not in self.filled:
            array = self.array_collection(shape).create()
            array[:].update(self.data(shape))
            self.filled[key] = array
        return self.filled[key]

    def filled_varray(self, shape: tuple[int, ...], vgrid: tuple[int, ...]) -> VArray:
        """Get the VArray of the shape and the grid holding ``data(shape)``, written on first use; it is never changed.

        :param shape: shape of the varray
        :param vgrid: grid of the varray
        :returns: the varray
        """
        key = ("filled-varray", shape, vgrid)
        if key not in self.filled:
            varray = self.varray_collection(shape, vgrid).create()
            varray[:].update(self.data(shape))
            self.filled[key] = varray
        return self.filled[key]

    def delete_all(self) -> None:
        """Delete every collection created so far."""
        self.filled.clear()
        for collection in self.collections.values():
            collection.delete()
        self.collections.clear()


@pytest.fixture(scope="session")
def stepped_collections(root_path) -> SteppedCollections:
    """Registry of the collections of the step tests, with its own client for the whole session."""
    client = Client(str(embedded_uri(root_path)), loglevel="ERROR")
    registry = SteppedCollections(client)
    try:
        yield registry
    finally:
        registry.delete_all()
        client.close()


@pytest.fixture
def stepped_array_collection(request, stepped_collections: SteppedCollections) -> Collection:
    """The Array collection of the shape of the parameter."""
    return stepped_collections.array_collection(request.param)


@pytest.fixture
def filled_stepped_array(request, stepped_collections: SteppedCollections) -> Array:
    """The Array of the shape of the parameter with data in every cell, shared by the reading tests."""
    return stepped_collections.filled_array(request.param)


@pytest.fixture
def stepped_varray_collection(request, stepped_collections: SteppedCollections) -> Collection:
    """The VArray collection of the shape and the grid of the parameter."""
    shape, vgrid = request.param
    return stepped_collections.varray_collection(shape, vgrid)


@pytest.fixture
def filled_stepped_varray(request, stepped_collections: SteppedCollections) -> VArray:
    """The VArray of the shape and the grid of the parameter with data in every cell, shared by the reading tests."""
    shape, vgrid = request.param
    return stepped_collections.filled_varray(shape, vgrid)
