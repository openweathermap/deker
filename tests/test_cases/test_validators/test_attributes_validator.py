import random

import pytest

from tests.parameters.collection_params import ClientParams

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


if __name__ == "__main__":
    pytest.main()
