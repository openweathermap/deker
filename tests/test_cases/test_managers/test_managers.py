import os

from datetime import datetime, timedelta

import pytest

from deker_tools.time import get_utc

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerFilterError
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)


class TestDataManagerMethods:
    def test_manager_create_array(self, collection_manager):
        """Tests if manager can create array.

        :param collection_manager: fixture
        """
        array = collection_manager.create()
        assert isinstance(array, Array)

    def test_get_array_by_id(self, inserted_array: Array, collection_manager):
        """Tests possibility of getting an array by id.

        :param inserted_array: fixture
        :param collection_manager: fixture
        """
        filter = collection_manager.filter({"id": inserted_array.id})
        array = filter.first()
        assert array
        assert array.id == inserted_array.id
        assert array.collection == inserted_array.collection
        assert array._vid == inserted_array._vid
        assert array._v_position == inserted_array._v_position

    def test_get_array_by_id_no_such_id(self, inserted_array: Array, collection_manager):
        """Tests if None is returned if there is no such id.

        :param inserted_array: fixture
        :param collection_manager: fixture
        """
        no_such_id = inserted_array.id + "1"
        filter = collection_manager.filter({"id": no_such_id})
        array = filter.first()
        assert array is None

    def test_get_array_by_id_empty_collection(self, array: Array, collection_manager):
        """Tests if None is returned if collection is empty.

        :param array: fixture
        :param collection_manager: fixture
        """
        filter = collection_manager.filter({"id": array.id})
        array = filter.first()
        assert array is None

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_array_by_primary_attribute(
        self,
        inserted_array_with_attributes: Array,
        collection_manager_with_attributes,
        property_name: str,
    ):
        """Tests possibility of getting an array by primary attributes.

        :param inserted_array_with_attributes: fixture
        :param collection_manager_with_attributes: fixture
        """

        getter = getattr(
            collection_manager_with_attributes.filter(
                {**inserted_array_with_attributes.primary_attributes}
            ),
            property_name,
        )
        array = getter()
        assert array
        assert array.id == inserted_array_with_attributes.id
        assert array.collection == inserted_array_with_attributes.collection
        assert array._vid == inserted_array_with_attributes._vid
        assert array._v_position == inserted_array_with_attributes._v_position

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_array_by_primary_attribute_datetime(
        self,
        client: Client,
        property_name: str,
    ):
        """Tests possibility of getting an array by datetme primary attributes.

        :param client: fixture
        :param property_name: option
        """
        dims = [
            TimeDimensionSchema(name="time", size=10, start_value="$dt", step=timedelta(hours=1)),
            DimensionSchema(name="y", size=10),
            DimensionSchema(name="x", size=10),
        ]
        attrs = [
            AttributeSchema(name="dt", dtype=datetime, primary=True),
            AttributeSchema(name="some", dtype=float, primary=False),
        ]
        schema = ArraySchema(dimensions=dims, attributes=attrs, dtype=float, fill_value=-9999)
        try:
            collection = client.create_collection("tm_collection", schema)
        except:
            client.get_collection("tm_collection")
            collection = client.create_collection("tm_collection", schema)

        try:
            now = datetime.now()
            primary_attr = {"dt": now}
            array = collection.create(primary_attr)
            assert array.primary_attributes == primary_attr
            assert get_utc(now).isoformat() in os.listdir(collection.path / "array_symlinks")

            getter = getattr(
                collection.filter({"dt": now.isoformat()}),
                property_name,
            )
            found_array = getter()
            assert found_array.id == array.id
        finally:
            collection.delete()

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_varray_by_primary_attribute_datetime(
        self,
        client: Client,
        property_name: str,
    ):
        """Tests possibility of getting an array by datetme primary attributes.

        :param client: fixture
        :param property_name: option
        """
        dims = [
            TimeDimensionSchema(name="time", size=10, start_value="$dt", step=timedelta(hours=1)),
            DimensionSchema(name="y", size=10),
            DimensionSchema(name="x", size=10),
        ]
        attrs = [
            AttributeSchema(name="dt", dtype=datetime, primary=True),
            AttributeSchema(name="some", dtype=float, primary=False),
        ]
        schema = VArraySchema(
            dimensions=dims, attributes=attrs, dtype=float, fill_value=-9999, vgrid=(1, 1, 1)
        )

        try:
            collection = client.create_collection("tm_collection", schema)
        except:
            client.get_collection("tm_collection")
            collection = client.create_collection("tm_collection", schema)

        try:
            now = datetime.now()
            primary_attr = {"dt": now}
            array = collection.create(primary_attr)
            assert array.primary_attributes == primary_attr
            assert get_utc(now).isoformat() in os.listdir(collection.path / "varray_symlinks")

            getter = getattr(
                collection.filter({"dt": now.isoformat()}),
                property_name,
            )
            found_array = getter()
            assert found_array.id == array.id
        finally:
            collection.delete()

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_array_by_primary_attribute_no_such_attribute(
        self,
        inserted_array_with_attributes: Array,
        collection_manager_with_attributes,
        property_name: str,
    ):
        """Tests None is returned if there is no array with such primary attributes.

        :param inserted_array_with_attributes: fixture
        :param collection_manager_with_attributes: fixture
        """
        attrs = {a: "no_such_value" for a in inserted_array_with_attributes.primary_attributes}
        getter = getattr(
            collection_manager_with_attributes.filter({**attrs}),
            property_name,
        )
        array = getter()
        assert array is None

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_array_by_primary_attribute_empty_collection(
        self,
        array_with_attributes: Array,
        collection_manager_with_attributes,
        property_name: str,
    ):
        """Tests None is returned if collection is empty.

        :param array_with_attributes: fixture
        :param collection_manager_with_attributes: fixture
        """
        getter = getattr(
            collection_manager_with_attributes.filter({**array_with_attributes.primary_attributes}),
            property_name,
        )
        array = getter()
        assert array is None

    def test_get_array_by_primary_attribute_fails_too_many_attrs(
        self,
        inserted_array_with_attributes: Array,
        collection_manager_with_attributes,
    ):
        """Tests if filtering fails of there is too many attributes

        :param inserted_array_with_attributes: Existing array
        :param collection_manager_with_attributes: Manager to test
        """
        with pytest.raises(DekerFilterError) as error:
            collection_manager_with_attributes.filter(
                {**inserted_array_with_attributes.primary_attributes, "extra": 12}
            ).first()
        assert "Some arguments don't exist in schema" == str(error.value)

        with pytest.raises(DekerFilterError) as error:
            collection_manager_with_attributes.filter(
                {**inserted_array_with_attributes.primary_attributes, "extra": 12}
            ).last()
        assert "Some arguments don't exist in schema" == str(error.value)

    def test_get_array_by_primary_attribute_absence(
        self,
        inserted_array_with_attributes: Array,
        collection_manager_with_attributes,
    ):
        with pytest.raises(NotImplementedError):
            collection_manager_with_attributes.filter({"extra": 12}).first()

        with pytest.raises(NotImplementedError):
            collection_manager_with_attributes.filter({"extra": 12}).last()


class TestDataManagerMethodsVArray:
    def test_manager_create_varray(self, va_collection_manager):
        """Tests if manager creates array.

        :param va_collection_manager: fixture
        """
        array = va_collection_manager.create()
        assert isinstance(array, VArray)

    def test_get_array_by_id(self, inserted_varray: VArray, va_collection_manager):
        """Tests possibility of getting an array by id.

        :param inserted_varray: fixture
        :param va_collection_manager: fixture
        """
        filter = va_collection_manager.filter({"id": inserted_varray.id})
        array = filter.first()
        assert array
        assert array.id == inserted_varray.id
        assert array.collection == inserted_varray.collection

    def test_get_varray_by_id_no_such_id(self, inserted_varray: VArray, va_collection_manager):
        """Tests if None is returned if there is no such id.

        :param inserted_varray: fixture
        :param va_collection_manager: fixture
        """
        no_such_id = inserted_varray.id + "1"
        filter = va_collection_manager.filter({"id": no_such_id})
        array = filter.first()
        assert array is None

    def test_get_varray_by_id_empty_collection(self, varray: VArray, va_collection_manager):
        """Tests if None is returned if collection is empty.

        :param varray: fixture
        :param va_collection_manager: fixture
        """
        filter = va_collection_manager.filter({"id": varray.id})
        array = filter.first()
        assert array is None

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_varray_by_primary_attribute(
        self,
        inserted_varray_with_attributes: VArray,
        va_collection_manager_with_attributes,
        property_name: str,
    ):
        """Tests possibility of getting an array by primary attributes.

        :param inserted_varray_with_attributes: fixture
        :param va_collection_manager_with_attributes: fixture
        """

        getter = getattr(
            va_collection_manager_with_attributes.filter(
                {**inserted_varray_with_attributes.primary_attributes}
            ),
            property_name,
        )
        array = getter()
        assert array
        assert array.id == inserted_varray_with_attributes.id
        assert array.collection == inserted_varray_with_attributes.collection

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_varray_by_primary_attribute_no_such_attribute(
        self,
        inserted_varray_with_attributes: VArray,
        va_collection_manager_with_attributes,
        property_name: str,
    ):
        """Tests None is returned if there is no array with such primary attributes.

        :param inserted_varray_with_attributes: fixture
        :param va_collection_manager_with_attributes: fixture
        """
        attrs = {a: "no_such_value" for a in inserted_varray_with_attributes.primary_attributes}
        getter = getattr(
            va_collection_manager_with_attributes.filter({**attrs}),
            property_name,
        )
        array = getter()
        assert array is None

    @pytest.mark.parametrize("property_name", ["first", "last"])
    def test_get_varray_by_primary_attribute_empty_collection(
        self,
        varray_with_attributes: VArray,
        va_collection_manager_with_attributes,
        property_name: str,
    ):
        """Tests None is returned if collection is empty.

        :param varray_with_attributes: fixture
        :param va_collection_manager_with_attributes: fixture
        """
        getter = getattr(
            va_collection_manager_with_attributes.filter(
                {**varray_with_attributes.primary_attributes}
            ),
            property_name,
        )
        array = getter()
        assert array is None

    def test_get_array_by_primary_attribute_fails_too_many_attrs(
        self,
        inserted_array_with_attributes: Array,
        va_collection_manager_with_attributes,
    ):
        """Tests if filtering fails if there is too many attributes

        :param inserted_array_with_attributes: Existing array
        :param va_collection_manager_with_attributes: Manager to test
        """
        with pytest.raises(DekerFilterError) as error:
            va_collection_manager_with_attributes.filter(
                {**inserted_array_with_attributes.primary_attributes, "extra": 12}
            ).first()
        assert "Some arguments don't exist in schema" == str(error.value)

        with pytest.raises(DekerFilterError) as error:
            va_collection_manager_with_attributes.filter(
                {**inserted_array_with_attributes.primary_attributes, "extra": 12}
            ).last()
        assert "Some arguments don't exist in schema" == str(error.value)

    def test_get_varray_by_primary_attribute_absence(
        self,
        inserted_varray_with_attributes: Array,
        va_collection_manager_with_attributes,
    ):
        with pytest.raises(NotImplementedError) as error:
            va_collection_manager_with_attributes.filter({"extra": 12}).first()

        with pytest.raises(NotImplementedError) as error:
            va_collection_manager_with_attributes.filter({"extra": 12}).last()


class TestArrayManager:
    def test_array_manager_in_varray_collection_create(self, varray_collection: Collection):
        """Tests if it is possible to create Array in VArray collection with array manager."""
        varray = varray_collection.create()
        varray[0, 0, 0:1].update([4.0])

        array = varray_collection.arrays.create({"vid": varray.id, "v_position": (0, 1, 0)})
        expected_array = varray_collection.arrays.filter(
            {"vid": varray.id, "v_position": (0, 1, 0)}
        ).last()
        assert array.id == expected_array.id
        assert array.primary_attributes == expected_array.primary_attributes


class TestVarrayManger:
    def test_varrays_manager_creates_varrays(self, varray_collection: Collection):
        varray = varray_collection.varrays.create()
        expected_array = varray_collection.varrays.filter({"id": varray.id}).last()
        assert varray.id == expected_array.id
        assert varray.primary_attributes == expected_array.primary_attributes


if __name__ == "__main__":
    pytest.main()
