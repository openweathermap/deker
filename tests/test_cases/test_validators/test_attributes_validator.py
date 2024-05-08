import random

import numpy as np
import pytest

from tests.parameters.collection_params import ClientParams

from deker import ArraySchema, AttributeSchema, DimensionSchema
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerCollectionAlreadyExistsError, DekerValidationError


@pytest.mark.asyncio()
class TestNoVgridValidateAttributes:
    """Tests for deker.validators.process_attributes."""

    @pytest.mark.parametrize(
        ("primary_attributes", "custom_attributes"),
        [
            ({"a": 1}, {}),
            ({}, {"a": 1}),
        ],
    )
    def test_no_attributes_schema(
        self, client: Client, primary_attributes: dict, custom_attributes: dict
    ):
        """Tests if schema raises error when attributes are not passed."""
        collection: Collection = client.create_collection(
            **ClientParams.ArraySchema.OK.no_vgrid_no_attrs()
        )
        try:
            with pytest.raises(DekerValidationError):
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
    def test_all_attributes_schema_extra_attrs(
        self, client: Client, primary_attributes: dict, custom_attributes: dict
    ):
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
    def test_primary_attributes_schema_attrs_no_attrs_provided(
        self, client: Client, primary_attributes: dict
    ):
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

    @pytest.mark.parametrize("custom_attributes", [None, {}])
    def test_custom_attributes_schema_attrs_no_attrs_provided(
        self, client: Client, custom_attributes: dict
    ):
        """Tests None and empty dict custom attributes return attributes=None."""
        coll_params = ClientParams.ArraySchema.OK.no_vgrid_custom_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            array = collection.create(custom_attributes=custom_attributes)
            for attr in coll_params["schema"].attributes:
                assert attr.name in array.custom_attributes
                assert array.custom_attributes[attr.name] is None
        finally:
            collection.delete()


@pytest.mark.asyncio()
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
    def test_all_attributes_schema_extra_attrs(
        self, client: Client, primary_attributes: dict, custom_attributes: dict
    ):
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
                collection.create(
                    primary_attributes=primary_attributes, custom_attributes=custom_attributes
                )
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
    def test_primary_attributes_schema_attrs_no_attrs_provided(
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

    @pytest.mark.parametrize("custom_attributes", [None, {}])
    def test_custom_attributes_schema_attrs_no_attrs_provided(
        self, client: Client, custom_attributes: dict
    ):
        """Tests None and empty dict custom attributes return attributes=None."""
        coll_params = ClientParams.ArraySchema.OK.time_params_no_vgrid_custom_attributes()
        try:
            collection: Collection = client.create_collection(**coll_params)
        except DekerCollectionAlreadyExistsError:
            coll = client.get_collection(coll_params["name"])
            coll.delete()
            collection: Collection = client.create_collection(**coll_params)
        try:
            array = collection.create(custom_attributes=custom_attributes)
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
            array = [a for a in col][0]
            attr = getattr(array, key)
            assert attr[schema.attributes[0].name] == value
        except Exception:
            raise
        finally:
            col.delete()


if __name__ == "__main__":
    pytest.main()
