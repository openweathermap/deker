import random
import uuid

from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from tests.parameters.collection_params import ClientParams

from deker import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema
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


class TestIsValidUuid:
    @pytest.mark.parametrize("value", [str(uuid.uuid4()), "12345678-1234-5678-1234-567812345678"])
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
