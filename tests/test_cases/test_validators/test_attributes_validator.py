import random
import uuid

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from tests.parameters.collection_params import ClientParams

from deker import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema, VArraySchema
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerCollectionAlreadyExistsError, DekerValidationError
from deker.validators import is_valid_uuid


@pytest.mark.asyncio
class TestNoVgridValidateAttributes:
    """Tests for deker.validators.process_attributes."""

    @pytest.mark.parametrize("missing", ["first", "second"])
    def test_one_of_two_primary_attributes_missing(self, client: Client, missing: str):
        """Tests a present primary attribute does not cover for a missing one: the missing one is named."""
        schema = ArraySchema(
            dtype=int,
            dimensions=[DimensionSchema(name="x", size=2)],
            attributes=[
                AttributeSchema(name="first", dtype=int, primary=True),
                AttributeSchema(name="second", dtype=int, primary=True),
            ],
        )
        collection = client.create_collection("one_primary_attribute_missing", schema)
        try:
            present = "second" if missing == "first" else "first"
            with pytest.raises(DekerValidationError, match=f"Key attribute missing: {missing}"):
                collection.create(primary_attributes={present: 1})
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
        ],
    )
    def test_no_attributes_schema(self, client: Client, primary_attributes: dict, custom_attributes: dict):
        """Tests if schema raises error when attributes are not passed."""
        collection: Collection = client.create_collection(**ClientParams.ArraySchema.OK.no_vgrid_no_attrs())
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_primary_attributes_schema_attrs_missing(self, client: Client):
        """Tests errors on primary attributes listed in schema but missing."""
        primary_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_primary_attributes()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = attr.dtype(1)
        keys = list(primary_attributes.keys())
        primary_attributes.pop(keys[random.randint(0, len(keys) - 1)])
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
        ],
    )
    def test_primary_attributes_schema_extra_attrs(
        self,
        client: Client,
        primary_attributes: dict,
        custom_attributes: dict,
    ):
        """Tests errors on extra attributes not listed in schema."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_primary_attributes()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = attr.dtype(1)
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
        ],
    )
    def test_custom_attributes_schema_extra_attrs(
        self, client: Client, primary_attributes: dict, custom_attributes: dict
    ):
        """Tests errors on extra attributes not listed in schema."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_custom_attributes()
        for attr in coll_params["schema"].attributes:
            custom_attributes[attr.name] = attr.dtype(1)
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
            ({"a": 1}, {"b": 2}),
        ],
    )
    def test_all_attributes_schema_extra_attrs(self, client: Client, primary_attributes: dict, custom_attributes: dict):
        """Tests errors on extra attributes not listed in schema."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_all_attrs()
        for attr in coll_params["schema"].attributes:
            if attr.primary:
                primary_attributes[attr.name] = attr.dtype(1)
            else:
                custom_attributes[attr.name] = attr.dtype(1)
        try:
            coll = client.get_collection(coll_params["name"])
            if coll:
                coll.delete()
        except Exception:
            pass
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_primary_custom_attributes_schema_same_names_attrs(self, client: Client):
        """Tests if can't create collection with the same names in primary and custom attributes."""
        primary_attributes = {}
        custom_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_all_attrs()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = attr.dtype(1)
            custom_attributes[attr.name] = attr.dtype(1)
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_primary_attributes_schema_attrs_wrong_dtype(self, client: Client):
        """Tests errors on extra attributes not listed in schema."""
        primary_attributes = {}
        custom_attributes = None
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_primary_attributes()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = {"a": 1, "b": 2}
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_custom_attributes_schema_attrs_wrong_dtype(self, client: Client):
        """Tests errors on attributes with invalid dtype."""
        primary_attributes = None
        custom_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_custom_attributes()
        for attr in coll_params["schema"].attributes:
            custom_attributes[attr.name] = {"a": 1, "b": 2}
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_all_attributes_schema_attrs_wrong_dtype(self, client: Client):
        """Tests errors on attributes with invalid dtype."""
        primary_attributes = {}
        custom_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_all_attrs()
        wrong_dtype_value = {"a": 1, "b": 2}
        for attr in coll_params["schema"].attributes:
            if attr.primary:
                primary_attributes[attr.name] = wrong_dtype_value
            else:
                custom_attributes[attr.name] = wrong_dtype_value
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            if coll:
                coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize("primary_attributes", [False, 0, "", [], tuple(), set()])
    def test_primary_attributes_schema_attrs_no_attrs_provided_false_objects(
        self, client: Client, primary_attributes: dict
    ):
        """Tests errors on no primary attributes provided."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_primary_attributes()
        try:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
        except AttributeError:
            pass

        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize("primary_attributes", [None, {}])
    def test_primary_attributes_schema_attrs_no_attrs_provided(self, client: Client, primary_attributes: dict):
        """Tests errors on no primary attributes provided."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_primary_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize("custom_attributes", [False, 0, "", [], tuple(), set()])
    def test_custom_attributes_schema_attrs_no_attrs_provided_false_objects(
        self, client: Client, custom_attributes: dict
    ):
        """Tests errors on no custom attributes provided."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_custom_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(custom_attributes=custom_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        "kwargs", [pytest.param({}, id="not-given"), pytest.param({"custom_attributes": {}}, id="empty")]
    )
    def test_custom_attributes_schema_attrs_no_attrs_provided(self, client: Client, kwargs: dict):
        """Tests custom attributes not given or empty give every custom attribute as None."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_custom_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            array = collection.create(**kwargs)
            for attr in coll_params["schema"].attributes:
                assert attr.name in array.custom_attributes
                assert array.custom_attributes[attr.name] is None
        finally:
            collection.delete()


@pytest.mark.asyncio
class TestNoVgridValidateAttributesTimeDimension:
    """Tests for deker.validators.process_attributes."""

    def test_primary_attributes_schema_attrs_missing(self, client: Client):
        """Tests errors on primary attributes listed in schema but missing."""
        primary_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_primary_attributes()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = attr.dtype(1)
        keys = list(primary_attributes.keys())
        primary_attributes.pop(keys[random.randint(0, len(keys) - 1)])
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
        ],
    )
    def test_primary_attributes_schema_extra_attrs(
        self,
        client: Client,
        primary_attributes: dict,
        custom_attributes: dict,
    ):
        """Tests errors on extra attributes not listed in schema."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_primary_attributes()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = attr.dtype(1)
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
        ],
    )
    def test_custom_attributes_schema_extra_attrs(
        self, client: Client, primary_attributes: dict, custom_attributes: dict
    ):
        """Tests errors on extra attributes not listed in schema."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_custom_attributes()
        for attr in coll_params["schema"].attributes:
            custom_attributes[attr.name] = attr.dtype(1)
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
            ({"a": 1}, {"b": 2}),
        ],
    )
    def test_all_attributes_schema_extra_attrs(self, client: Client, primary_attributes: dict, custom_attributes: dict):
        """Tests errors on extra attributes not listed in schema."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_all_attrs()
        for attr in coll_params["schema"].attributes:
            if attr.primary:
                primary_attributes[attr.name] = attr.dtype(1)
            else:
                custom_attributes[attr.name] = attr.dtype(1)
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_primary_attributes_schema_same_names_attrs(self, client: Client):
        """Tests errors on attributes have same names."""
        primary_attributes = {}
        custom_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_all_attrs()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = attr.dtype(1)
            custom_attributes[attr.name] = attr.dtype(1)
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_primary_attributes_schema_attrs_wrong_dtype(self, client: Client):
        """Tests errors on attributes with invalid dtype."""
        primary_attributes = {}
        custom_attributes = None
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_primary_attributes()
        for attr in coll_params["schema"].attributes:
            primary_attributes[attr.name] = {"a": 1, "b": 2}
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_custom_attributes_schema_attrs_wrong_dtype(self, client: Client):
        """Tests errors on attributes with invalid dtype."""
        primary_attributes = None
        custom_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_custom_attributes()
        for attr in coll_params["schema"].attributes:
            custom_attributes[attr.name] = {"a": 1, "b": 2}
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    def test_all_attributes_schema_attrs_wrong_dtype(self, client: Client):
        """Tests errors on attributes with invalid dtype."""
        primary_attributes = {}
        custom_attributes = {}
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_all_attrs()
        wrong_dtype_value = {"a": 1, "b": 2}
        for attr in coll_params["schema"].attributes:
            if attr.primary:
                primary_attributes[attr.name] = wrong_dtype_value
            else:
                custom_attributes[attr.name] = wrong_dtype_value
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes, custom_attributes=custom_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize("primary_attributes", [False, 0, "", [], tuple(), set()])
    def test_primary_attributes_schema_attrs_no_attrs_provided_false_objects(
        self, client: Client, primary_attributes: dict
    ):
        """Tests errors on no primary attributes provided."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_primary_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize("primary_attributes", [None, {}])
    def test_primary_attributes_schema_attrs_no_attrs_provided(self, client: Client, primary_attributes: dict):
        """Tests errors on no primary attributes provided."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_primary_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            with pytest.raises(DekerValidationError):
                collection.create(primary_attributes=primary_attributes)
        finally:
            collection.delete()

    @pytest.mark.parametrize("custom_attributes", [False, 0, "", [], tuple(), set()])
    def test_custom_attributes_schema_attrs_no_attrs_provided_false_objects(
        self, client: Client, custom_attributes: dict
    ):
        """Tests errors on no custom attributes provided."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_custom_attributes()
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

    @pytest.mark.parametrize(
        "kwargs", [pytest.param({}, id="not-given"), pytest.param({"custom_attributes": {}}, id="empty")]
    )
    def test_custom_attributes_schema_attrs_no_attrs_provided(self, client: Client, kwargs: dict):
        """Tests custom attributes not given or empty give every custom attribute as None."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_custom_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            array = collection.create(**kwargs)
            for attr in coll_params["schema"].attributes:
                assert attr.name in array.custom_attributes
                assert array.custom_attributes[attr.name] is None
        finally:
            collection.delete()


class TestNoAttributes:
    def test_no_attributes(self, client):
        """Test that it's possible to create array with empty primary and custom attributes schema."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_no_attrs()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            array = collection.create()
            assert array
        finally:
            collection.delete()


class TestAttributesValues:
    @pytest.mark.parametrize("primary", [False, True])
    @pytest.mark.parametrize(
        ("dtype", "value"),
        [
            (str, "123"),
            (
                str,
                "-125.000000000001-0.123456789j",
            ),  # it's not an exact string representation of a complex number
            (np.int8, np.int8(1)),
            (np.int16, np.int16(-130)),
            (np.int32, np.int32(-9999)),
            (np.int64, np.int64(99999999)),
            (int, 1),
            (int, 0),
            (int, -1),
            (float, 0.1),
            (float, -0.1),
            (np.float16, np.float16(1.0)),
            (np.float32, np.float32(-130)),
            (np.float64, np.float64(-9999)),
            (np.float128, np.float128(99999999)),
            (complex, complex(0.0000000000001, 9.000000005)),
            (complex, complex(-0.0000000000001, -1.000000009)),
            (np.complex64, np.complex64(1.0)),
            (np.complex128, np.complex128(-130)),
            (np.complex256, np.complex256(-9999)),
            (tuple, tuple("abc")),
            (tuple, tuple({"abc", "def"})),
            (tuple, (1, 2, 3, 4)),
            (
                tuple,
                (
                    np.int8(1),
                    np.int16(-130),
                    np.int32(-9999),
                    np.int64(99999999),
                    np.float16(1.0),
                    np.float32(-130),
                    np.float64(-9999),
                    np.float128(99999999),
                    np.complex64(1.0),
                    np.complex128(-130),
                    np.complex256(-9999),
                ),
            ),
            (
                tuple,
                (
                    1,
                    0.1,
                    complex(0.0000000000001, 0.0000000000001),
                    complex(-0.00000000000012567, -0.0000000000001),
                    -0.1,
                    -1,
                ),
            ),
            (tuple, (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1)),
            (
                tuple,
                (
                    (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1),
                    (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1),
                ),
            ),
            (
                tuple,
                (
                    (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1),
                    (1, 0.1, complex(0.0000000000001), complex(-0.0000000000001), -0.1, -1),
                ),
            ),
        ],
    )
    def test_attributes_values_serialize_deserialize_ok(self, client, primary, dtype, value):
        schema = ArraySchema(
            dimensions=[DimensionSchema(name="x", size=1)],
            dtype=int,
            attributes=[AttributeSchema(name="some_attr", dtype=dtype, primary=primary)],
        )
        col_name = "test_attrs_values_validation"
        try:
            col = client.create_collection(col_name, schema)
        except DekerCollectionAlreadyExistsError:
            col = client.get_collection(col_name)
            col.clear()
        try:
            if primary:
                key = "primary_attributes"
            else:
                key = "custom_attributes"
            attrs = {key: {schema.attributes[0].name: value}}
            array = col.create(**attrs)
            assert array
            array = [*col][0]
            attr = getattr(array, key)
            assert attr[schema.attributes[0].name] == value
        except Exception:
            raise
        finally:
            col.delete()


if __name__ == "__main__":
    pytest.main()


class TestCustomAttributesUpdateWithTimeDimension:
    """validate_custom_attributes_update keeps the start of a $-time dimension consistent on update."""

    @pytest.fixture
    def collection_with_primary_time(self, client: Client) -> Collection:
        schema = ArraySchema(
            dtype=float,
            dimensions=[TimeDimensionSchema(name="t", size=4, start_value="$start", step=timedelta(hours=1))],
            attributes=[
                AttributeSchema(name="start", dtype=datetime, primary=True),
                AttributeSchema(name="note", dtype=str, primary=False),
            ],
        )
        collection = client.create_collection("update_primary_time", schema)
        try:
            yield collection
        finally:
            collection.delete()

    @pytest.fixture
    def collection_with_custom_time(self, client: Client) -> Collection:
        schema = ArraySchema(
            dtype=float,
            dimensions=[TimeDimensionSchema(name="t", size=4, start_value="$start", step=timedelta(hours=1))],
            attributes=[
                AttributeSchema(name="start", dtype=datetime, primary=False),
                AttributeSchema(name="note", dtype=str, primary=False),
            ],
        )
        collection = client.create_collection("update_custom_time", schema)
        try:
            yield collection
        finally:
            collection.delete()

    def test_primary_time_attribute_is_untouched_by_a_custom_update(self, collection_with_primary_time: Collection):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        array = collection_with_primary_time.create({"start": start}, {"note": "old"})
        array.update_custom_attributes({"note": "new"})
        assert array.custom_attributes == {"note": "new"}
        assert array.primary_attributes == {"start": start}
        assert array.dimensions[0].start_value == start

    def test_custom_time_attribute_not_in_the_update_is_kept_from_the_dimension(
        self, collection_with_custom_time: Collection
    ):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        array = collection_with_custom_time.create(custom_attributes={"start": start, "note": "old"})
        array.update_custom_attributes({"note": "new"})
        assert array.custom_attributes == {"note": "new", "start": start}
        assert array.dimensions[0].start_value == start

    def test_custom_time_attribute_in_the_update_moves_the_dimension(self, collection_with_custom_time: Collection):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        later = datetime(2024, 2, 1, tzinfo=timezone.utc)
        array = collection_with_custom_time.create(custom_attributes={"start": start, "note": "old"})
        array.update_custom_attributes({"start": later})
        assert array.custom_attributes == {"note": "old", "start": later}
        assert array.dimensions[0].start_value == later


class TestTupleAttributesWithNone:
    """A tuple attribute may hold None among its elements: there is nothing to type-check in None."""

    def test_tuple_with_none_is_accepted_and_stored(self, client: Client):
        schema = ArraySchema(
            dtype=float,
            dimensions=[DimensionSchema(name="x", size=2)],
            attributes=[
                AttributeSchema(name="key", dtype=tuple, primary=True),
                AttributeSchema(name="extra", dtype=tuple, primary=False),
            ],
        )
        collection = client.create_collection("tuple_with_none", schema)
        try:
            array = collection.create({"key": (1, None, "a")}, {"extra": ((2.5, None), None)})
            assert array.primary_attributes == {"key": (1, None, "a")}
            assert array.custom_attributes == {"extra": ((2.5, None), None)}
            stored = collection.filter({"key": (1, None, "a")}).last()
            assert stored is not None
            assert stored.id == array.id
            assert stored.custom_attributes == {"extra": ((2.5, None), None)}
        finally:
            collection.delete()

    @pytest.mark.parametrize("value", [(1, object()), (1, (2, b"x")), ([1, 2],)])
    def test_tuple_with_a_value_of_an_unknown_type_is_rejected(self, client: Client, value: tuple):
        schema = ArraySchema(
            dtype=float,
            dimensions=[DimensionSchema(name="x", size=2)],
            attributes=[AttributeSchema(name="key", dtype=tuple, primary=True)],
        )
        collection = client.create_collection("tuple_with_unknown", schema)
        try:
            with pytest.raises(DekerValidationError, match="Invalid dtype value"):
                collection.create({"key": value})
        finally:
            collection.delete()


class TestAttributesInTheirOwnDictionary:
    """Every attribute of the schema is given in its own dictionary, for an Array and a VArray alike.

    The schema: primary ``a`` and ``b``, custom ``note`` and ``extra``.
    """

    WRONG = "Attributes passed in the wrong dictionary: "

    @staticmethod
    def make_collection(client: Client, kind: str) -> Collection:
        """Create the collection of the kind with the attributes of the class.

        :param client: client of the test
        :param kind: ``Array`` or ``VArray``
        :returns: the collection
        """
        attributes = [
            AttributeSchema(name="a", dtype=int, primary=True),
            AttributeSchema(name="b", dtype=int, primary=True),
            AttributeSchema(name="note", dtype=str, primary=False),
            AttributeSchema(name="extra", dtype=int, primary=False),
        ]
        dimensions = [DimensionSchema(name="x", size=4)]
        if kind == "VArray":
            schema = VArraySchema(dtype=float, dimensions=dimensions, vgrid=(2,), attributes=attributes)
        else:
            schema = ArraySchema(dtype=float, dimensions=dimensions, attributes=attributes)
        return client.create_collection(f"own_dictionary_{kind.lower()}", schema)

    @staticmethod
    def create(collection: Collection, kind: str, way: str, primary_attributes: dict, custom_attributes: dict):
        """Create an array by one of the public ways.

        :param collection: the collection
        :param kind: ``Array`` or ``VArray``
        :param way: ``collection`` - ``collection.create``; ``manager`` - ``collection.arrays.create`` or
          ``collection.varrays.create``
        :param primary_attributes: primary attributes
        :param custom_attributes: custom attributes
        :returns: the created array
        """
        # copies: DEKER completes the given custom attributes in place, and the parameters are shared by the cases
        primary_attributes, custom_attributes = dict(primary_attributes), dict(custom_attributes)
        if way == "collection":
            return collection.create(primary_attributes, custom_attributes)
        manager = collection.varrays if kind == "VArray" else collection.arrays
        return manager.create(primary_attributes, custom_attributes)

    @pytest.mark.parametrize("way", ["collection", "manager"])
    @pytest.mark.parametrize("kind", ["Array", "VArray"])
    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes", "expected_custom"),
        [
            ({"a": 1, "b": 2}, {"note": "kept"}, {"note": "kept", "extra": None}),
            ({"b": 2, "a": 1}, {"extra": 3, "note": "kept"}, {"note": "kept", "extra": 3}),
            ({"a": 1, "b": 2}, {}, {"note": None, "extra": None}),
        ],
    )
    def test_attributes_in_their_own_dictionaries_are_accepted(
        self,
        client: Client,
        kind: str,
        way: str,
        primary_attributes: dict,
        custom_attributes: dict,
        expected_custom: dict,
    ):
        collection = self.make_collection(client, kind)
        try:
            array = self.create(collection, kind, way, primary_attributes, custom_attributes)
            assert type(array).__name__ == kind
            assert array.primary_attributes == {"a": 1, "b": 2}
            assert list(array.primary_attributes) == ["a", "b"]
            assert array.custom_attributes == expected_custom
            stored = collection.filter({"a": 1, "b": 2}).last()
            assert stored is not None
            assert stored.id == array.id
            assert stored.primary_attributes == {"a": 1, "b": 2}
            assert stored.custom_attributes == expected_custom
        finally:
            collection.delete()

    @pytest.mark.parametrize("way", ["collection", "manager"])
    @pytest.mark.parametrize("kind", ["Array", "VArray"])
    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes", "message"),
        [
            pytest.param(
                {"a": 1, "b": 2, "note": "lost"},
                {},
                WRONG + "['note'] shall be in custom attributes",
                id="custom_among_primary",
            ),
            pytest.param(
                {"a": 1, "b": 2, "note": "lost", "extra": 3},
                {},
                WRONG + "['extra', 'note'] shall be in custom attributes",
                id="two_custom_among_primary",
            ),
            pytest.param(
                {"a": 1},
                {"b": 2, "note": "kept"},
                WRONG + "['b'] shall be in primary attributes",
                id="primary_among_custom",
            ),
            pytest.param(
                {"a": 1, "note": "lost"},
                {"b": 2},
                WRONG + "['note'] shall be in custom attributes; ['b'] shall be in primary attributes",
                id="custom_among_primary_and_primary_among_custom",
            ),
            pytest.param(
                {"a": 1, "b": 2, "note": "in primary"},
                {"note": "apart"},
                "Key and custom attributes shall not have same names; invalid names: {'note'}",
                id="custom_in_both",
            ),
            pytest.param(
                {"a": 1, "b": 2},
                {"a": 1, "note": "kept"},
                "Key and custom attributes shall not have same names; invalid names: {'a'}",
                id="primary_in_both",
            ),
            pytest.param({"a": 1}, {}, "Key attribute missing: b", id="primary_missing"),
            pytest.param({}, {"a": 1, "b": 2}, "No primary attributes provided", id="all_primary_among_custom"),
            pytest.param(
                {"a": 1, "b": 2, "zzz": 1},
                {},
                "Setting additional attributes not listed in schema is not allowed. Invalid attributes: ['zzz']",
                id="unknown_among_primary",
            ),
            pytest.param(
                {"a": 1, "b": 2},
                {"zzz": 1},
                "Setting additional attributes not listed in schema is not allowed. Invalid attributes: ['zzz']",
                id="unknown_among_custom",
            ),
            pytest.param(
                {"a": 1, "b": 2, "note": "lost", "zzz": 1},
                {},
                "Setting additional attributes not listed in schema is not allowed. Invalid attributes: ['zzz']",
                id="unknown_and_custom_among_primary",
            ),
        ],
    )
    def test_attributes_out_of_their_own_dictionaries_are_rejected(
        self,
        client: Client,
        kind: str,
        way: str,
        primary_attributes: dict,
        custom_attributes: dict,
        message: str,
    ):
        collection = self.make_collection(client, kind)
        try:
            with pytest.raises(DekerValidationError) as error:
                self.create(collection, kind, way, primary_attributes, custom_attributes)
            assert str(error.value) == message
            assert list(collection) == []
            assert list(collection.arrays) == []
            if kind == "VArray":
                assert list(collection.varrays) == []
        finally:
            collection.delete()

    def test_arrays_of_a_varray_get_their_attributes_as_before(self, client: Client):
        """Writing a VArray creates its Arrays with vid, v_position and the time attribute; reading finds them."""
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        step = timedelta(hours=1)
        schema = VArraySchema(
            dtype=float,
            dimensions=[TimeDimensionSchema(name="t", size=4, start_value="$dt", step=step)],
            vgrid=(2,),
            attributes=[
                AttributeSchema(name="a", dtype=int, primary=True),
                AttributeSchema(name="dt", dtype=datetime, primary=False),
            ],
        )
        collection = client.create_collection("own_dictionary_inner_arrays", schema)
        try:
            varray = collection.create({"a": 1}, {"dt": start})
            data = np.arange(4, dtype=float)
            varray[:].update(data)
            arrays = {array.primary_attributes["v_position"]: array for array in collection.arrays}
            assert sorted(arrays) == [(0,), (1,)]
            for (position,), array in arrays.items():
                assert array.primary_attributes == {"vid": varray.id, "v_position": (position,)}
                assert array.custom_attributes == {"parent.dt": start + step * 2 * position}
                assert (array[:].read() == data[position * 2 : position * 2 + 2]).all()
            assert (varray[:].read() == data).all()
            varray[2:].update(np.full(2, 9.0))
            assert (varray[:].read() == np.array([0.0, 1.0, 9.0, 9.0])).all()
            assert len(list(collection.arrays)) == 2
        finally:
            collection.delete()


class TestDictionariesOfAnAttributeError:
    """``DekerValidationError.parameters`` names the dictionaries of the attributes an error is in.

    The schema: primary ``a`` (int) and ``pt`` (tuple); custom ``dt`` (datetime, the start of the time dimension),
    ``note`` (str) and ``ct`` (tuple). The texts of the errors are those of before.
    """

    START = datetime(2026, 1, 1, tzinfo=timezone.utc)
    PRIMARY = ("primary_attributes",)
    CUSTOM = ("custom_attributes",)
    BOTH = ("primary_attributes", "custom_attributes")
    EXTRA = "Setting additional attributes not listed in schema is not allowed. Invalid attributes: "
    WRONG = "Attributes passed in the wrong dictionary: "
    SAME = "Key and custom attributes shall not have same names; invalid names: {'a'}"
    NOTE_TYPE = "Custom attribute \"note\" invalid type <class 'int'>; expected <class 'str'>"
    START_NONE = 'Custom attribute "dt" cannot be None'
    LIST_IN_TUPLE = "Invalid dtype value <class 'list'>"

    @staticmethod
    def make_collection(client: Client, kind: str, with_attributes: bool = True) -> Collection:
        """Create the collection of the kind.

        :param client: client of the test
        :param kind: ``Array`` or ``VArray``
        :param with_attributes: ``False`` - a schema without attributes and without the time dimension
        :returns: the collection
        """
        dimensions: list = [DimensionSchema(name="x", size=4)]
        attributes = []
        if with_attributes:
            dimensions.append(TimeDimensionSchema(name="t", size=2, start_value="$dt", step=timedelta(hours=1)))
            attributes = [
                AttributeSchema(name="a", dtype=int, primary=True),
                AttributeSchema(name="pt", dtype=tuple, primary=True),
                AttributeSchema(name="dt", dtype=datetime, primary=False),
                AttributeSchema(name="note", dtype=str, primary=False),
                AttributeSchema(name="ct", dtype=tuple, primary=False),
            ]
        if kind == "VArray":
            vgrid = (2, 1) if with_attributes else (2,)
            schema = VArraySchema(dtype=float, dimensions=dimensions, vgrid=vgrid, attributes=attributes)
        else:
            schema = ArraySchema(dtype=float, dimensions=dimensions, attributes=attributes)
        return client.create_collection(f"error_dictionaries_{kind.lower()}_{with_attributes}", schema)

    @pytest.mark.parametrize(("kind", "message"), [("Array", "Array"), ("VArray", "Varray")])
    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes", "parameters"),
        [
            pytest.param({"a": 1}, {}, PRIMARY, id="primary"),
            pytest.param({}, {"note": "x"}, CUSTOM, id="custom"),
            pytest.param({"a": 1}, {"note": "x"}, BOTH, id="both"),
        ],
    )
    def test_attributes_of_a_schema_without_attributes(
        self,
        client: Client,
        kind: str,
        message: str,
        primary_attributes: dict,
        custom_attributes: dict,
        parameters: tuple,
    ):
        collection = self.make_collection(client, kind, with_attributes=False)
        try:
            with pytest.raises(DekerValidationError) as error:
                collection.create(dict(primary_attributes), dict(custom_attributes))
            assert str(error.value) == f"{message} attributes schema is missing"
            assert error.value.parameters == parameters
            assert list(collection) == []
        finally:
            collection.delete()

    @pytest.mark.parametrize("kind", ["Array", "VArray"])
    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes", "message", "parameters"),
        [
            pytest.param({}, {"dt": START}, "No primary attributes provided", PRIMARY, id="no_primary"),
            pytest.param({"a": 1, "pt": (1,)}, {"a": 1, "dt": START}, SAME, BOTH, id="same_names"),
            pytest.param({"a": 1, "pt": (1,), "zzz": 1}, {"dt": START}, EXTRA + "['zzz']", PRIMARY, id="extra_primary"),
            pytest.param({"a": 1, "pt": (1,)}, {"dt": START, "zzz": 1}, EXTRA + "['zzz']", CUSTOM, id="extra_custom"),
            pytest.param(
                {"a": 1, "pt": (1,), "zzz": 1}, {"dt": START, "yyy": 1}, EXTRA + "['yyy', 'zzz']", BOTH, id="extra_both"
            ),
            pytest.param(
                {"a": 1, "pt": (1,), "note": "x"},
                {"dt": START},
                WRONG + "['note'] shall be in custom attributes",
                PRIMARY,
                id="custom_among_primary",
            ),
            pytest.param(
                {"a": 1},
                {"pt": (1,), "dt": START},
                WRONG + "['pt'] shall be in primary attributes",
                CUSTOM,
                id="primary_among_custom",
            ),
            pytest.param(
                {"a": 1, "note": "x"},
                {"pt": (1,), "dt": START},
                WRONG + "['note'] shall be in custom attributes; ['pt'] shall be in primary attributes",
                BOTH,
                id="both_misplaced",
            ),
            pytest.param({"a": 1}, {"dt": START}, "Key attribute missing: pt", PRIMARY, id="primary_missing"),
            pytest.param(
                {"a": "1", "pt": (1,)},
                {"dt": START},
                "Key attribute \"a\" invalid type: <class 'str'>; expected <class 'int'>",
                PRIMARY,
                id="primary_type",
            ),
            pytest.param({"a": 1, "pt": ([1],)}, {"dt": START}, LIST_IN_TUPLE, PRIMARY, id="primary_tuple_element"),
            pytest.param({"a": 1, "pt": (1,)}, {"dt": START, "note": 1}, NOTE_TYPE, CUSTOM, id="custom_type"),
            pytest.param({"a": 1, "pt": (1,)}, {}, START_NONE, CUSTOM, id="custom_time_start_none"),
            pytest.param(
                {"a": 1, "pt": (1,)}, {"dt": START, "ct": ([1],)}, LIST_IN_TUPLE, CUSTOM, id="custom_tuple_element"
            ),
        ],
    )
    def test_creation_error_names_its_dictionaries(
        self,
        client: Client,
        kind: str,
        primary_attributes: dict,
        custom_attributes: dict,
        message: str,
        parameters: tuple,
    ):
        collection = self.make_collection(client, kind)
        try:
            with pytest.raises(DekerValidationError) as error:
                collection.create(dict(primary_attributes), dict(custom_attributes))
            assert str(error.value) == message
            assert error.value.parameters == parameters
            assert list(collection) == []
        finally:
            collection.delete()

    @pytest.mark.parametrize("kind", ["Array", "VArray"])
    @pytest.mark.parametrize(
        ("attributes", "message"),
        [
            pytest.param({}, "No attributes passed for update", id="nothing"),
            pytest.param({"zzz": 1}, EXTRA + "['zzz']", id="extra"),
            pytest.param({"a": 5}, SAME, id="primary_name"),
            pytest.param({"note": 1}, NOTE_TYPE, id="custom_type"),
            pytest.param({"dt": None}, START_NONE, id="custom_time_start_none"),
            pytest.param({"ct": ([1],)}, LIST_IN_TUPLE, id="custom_tuple_element"),
        ],
    )
    def test_update_error_names_the_custom_attributes(self, client: Client, kind: str, attributes: dict, message: str):
        """The update takes the custom attributes only: every error of it is in them, a primary name too."""
        collection = self.make_collection(client, kind)
        try:
            array = collection.create({"a": 1, "pt": (1,)}, {"dt": self.START})
            with pytest.raises(DekerValidationError) as error:
                array.update_custom_attributes(dict(attributes))
            assert str(error.value) == message
            assert error.value.parameters == self.CUSTOM
            assert array.custom_attributes == {"dt": self.START, "note": None, "ct": None}
        finally:
            collection.delete()


class TestIsValidUuid:
    # a fixed id: every worker of pytest-xdist collects the tests itself, and a random value in the id differs
    @pytest.mark.parametrize(
        "value", [pytest.param(str(uuid.uuid4()), id="random"), "12345678-1234-5678-1234-567812345678"]
    )
    def test_valid(self, value: str):
        assert is_valid_uuid(value) is True

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("12345678-1234-5678-1234-56781234567g", id="not-hex"),
            pytest.param("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz", id="letters"),
            pytest.param("12345678123456781234567812345678", id="no-dashes"),
            pytest.param("1234-5678", id="short"),
            pytest.param("", id="empty"),
            pytest.param(None, id="none"),
            pytest.param(uuid.uuid4(), id="uuid-object"),
            pytest.param(12345678123456781234567812345678, id="int"),
        ],
    )
    def test_invalid(self, value):
        assert is_valid_uuid(value) is False
