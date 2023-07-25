from typing import TYPE_CHECKING, Optional, Tuple

from deker.ABC.base_array import BaseArray
from deker.schemas import ArraySchema, VArraySchema


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.collection import Collection


class Array(BaseArray):
    """Array is an abstract wrapper over final low-level arrays containing data.

    Array is being created by collection. It is a "lazy" object that does not interact with its data itself,
    but it possesses all information about the array. Interaction with data is performed by a Subset object,
    which is being produced from Array by its __getitem__ method.

    Getter-properties:
        - as_dict: serializes main information about array into dictionary, prepared for JSON
        - collection: returns the name of collection to which the array is bound
        - dimensions: returns a tuple of array's dimensions as objects
        - dtype: returns type of the array data
        - id: returns array's id
        - schema: returns array low-level schema
        - shape: returns array shape as a tuple of dimension sizes
        - named_shape: returns array shape as a tuple of dimension names bound to their sizes
        - vid: returns virtual array id, if the array is bound to any virtual array, else None
        - vposition: returns array's position in virtual array, if the array is bound to any virtual array,
            else None

    Attributes:
        According to the schema, arrays may have or may not have primary attributes and/or custom attributes.
        Attributes are stored in low-level array's metadata. Key attributes are immutable and ordered,
        custom attributes are mutable and unordered.

        - custom_attributes: Custom attributes are being used to keep some unique information about the array
         or about its data. E.g. you store some weather data, which is being calculated at different heights.
         If it is not important for filtering you can keep the information about certain heights in arrays'
         custom attribute, named "height". In this case you won't be able to search by this attribute,
         but still you'll be able to use this information in your calculations.
         You can set this attribute to None anytime for any array as it is not a global setting.
         But you are not able to change dtype of custom attributes. In other words you can not provide attribute's
         dtype as `int` and put there any `float` or string or whatever.

        - primary_attributes: Key attributes are being used for filtering arrays. It is an OrderedDict.
        If collection schema contains attributes schema with some of them as `primary`, you obtain
        a possibility to quickly find all arrays which can have a certain attribute or meet same
        conditions based on primary attributes values.

    API Methods:
        - read_meta: reads the array's metadata from storage
        - update_custom_attributes: updates array's `custom_attributes` attribute and metadata on the storage
        - delete: deletes array from the storage with all its data and metadata
        - __getitem__: returns a Subset instance bound to the array
        - __repr__: ordinary behaviour
        - __str__: ordinary behaviour
    """

    __slots__ = (
        "__id",
        "custom_attributes",
        "primary_attributes",
    )

    def __init__(
        self,
        collection: "Collection",
        adapter: "BaseArrayAdapter",
        id_: Optional[str] = None,
        primary_attributes: Optional[dict] = None,
        custom_attributes: Optional[dict] = None,
    ) -> None:
        """Array initialization.

        :param collection: instance of the collection, to which the array is bound
        :param adapter: Array adapter instance
        :param id_: array unique uuid string
        :param primary_attributes: primary attributes keyword mapping
        :param custom_attributes: custom attributes keyword mapping
        """
        self.__collection = collection
        self.__adapter = adapter
        super().__init__(
            collection,
            self.__adapter,
            id_=id_,
            primary_attributes=primary_attributes,
            custom_attributes=custom_attributes,
        )

    @property
    def _adapter(self) -> "BaseArrayAdapter":
        return self.__adapter  # type: ignore[return-value]

    @property
    def _vid(self) -> Optional[str]:
        """Get id of the virtual array to which the array is bound."""
        if self.__collection.varray_schema:
            return self.primary_attributes.get("vid")

    @property
    def _v_position(self) -> Optional[Tuple[int, ...]]:
        """Get array position in virtual array."""
        if self.__collection.varray_schema:
            vpos = self.primary_attributes.get("v_position")
            return tuple(vpos) if vpos else vpos

    @property
    def schema(self) -> ArraySchema:
        """Return ArraySchema of collection."""
        return self.__collection.array_schema


class VArray(BaseArray):
    """VArray is an abstract virtual array that consists of ordinary Arrays.

    Virtual array is an "array of arrays" or an "image of pixels". If we consider VArray as an image -
    it is being split by virtual grid into tiles. In this case, each tile is an ordinary Array.
    Meanwhile, for user virtual array acts as an ordinary array

    VArray is a "lazy" object that does not interact with its data itself,
    but it possesses all information about the virtual array (and even a bit more). Interaction with data
    is performed by a VSubset object, which is being produced from VArray by its __getitem__ method.

    Getter-properties:
        - as_dict: serializes main information about virtual array into dictionary, prepared for JSON
        - arrays_shape: common shape of all the Arrays bound to the virtual array.
        - collection: returns the name of collection to which the virtual array is bound
        - dimensions: returns a tuple of array's dimensions as objects
        - dtype: returns type of the array data
        - id: returns virtual array's id
        - schema: returns virtual array low-level schema
        - shape: returns virtual array shape as a tuple of dimension sizes
        - named_shape: returns virtual array shape as a tuple of dimension names bound to their sizes
        - vgrid: returns virtual grid (a tuple of integers) by which virtual array is being split into arrays

    Attributes:
        According to the schema, arrays may have or may not have primary attributes and/or custom attributes.
        Attributes are stored in low-level array's metadata. Key attributes are immutable and ordered,
        custom attributes are mutable and unordered.

        - custom_attributes: Custom attributes are being used to keep some unique information about the array
            or about its data. E.g., you store some weather data, which is being calculated at different heights.
            If it is not important for filtering, you can keep the information about certain heights in arrays'
            custom attribute, named "height". In this case, you won't be able to search by this attribute,
            but still you'll be able to use this information in your calculations, checking it array by array.
            Anytime you can set this attribute to None for any array as it is not a global setting.
            But you are not able to change dtype of custom attributes. In other words, you can not provide attribute's
            dtype as `int` and set there any `float` or string or whatever.

        - primary_attributes: Key attributes are being used for filtering arrays. It is an OrderedDict.
            If collection schema contains attributes schema with some of them as `primary`, you obtain
            a possibility to quickly find all arrays which can have a certain attribute or meet the same
            conditions based on primary attributes values.

    API Methods:
        - read_meta: reads the array's metadata from storage
        - update_custom_attributes: updates array's `custom_attributes` attribute and metadata on the storage
        - delete: deletes array from the storage with all its data and metadata
        - __getitem__: returns a Subset instance bound to the array
        - __repr__: ordinary behaviour
        - __str__: ordinary behaviour
    """

    __slots__ = (
        "__id",
        "custom_attributes",
        "primary_attributes",
        "__adapter",
    )

    def __init__(
        self,
        collection: "Collection",
        adapter: "BaseVArrayAdapter",
        array_adapter: "BaseArrayAdapter",
        id_: Optional[str] = None,
        primary_attributes: Optional[dict] = None,
        custom_attributes: Optional[dict] = None,
    ) -> None:
        """VArray initialization.

        :param collection: instance of the collection, to which the virtual array is bound
        :param adapter: VArray adapter instance
        :param array_adapter: Array adapter instance
        :param id_: virtual array unique uuid string
        :param primary_attributes: any primary attribute keyword mapping
        :param custom_attributes: any custom attributes keyword mapping
        """
        self.__collection = collection
        self.__adapter = adapter
        self.__array_adapter = array_adapter
        super().__init__(
            collection,
            id_=id_,
            adapter=adapter,
            array_adapter=array_adapter,
            primary_attributes=primary_attributes,
            custom_attributes=custom_attributes,
        )

    @property
    def _adapter(self) -> "BaseVArrayAdapter":
        return self.__adapter  # type: ignore[return-value]

    @property
    def vgrid(self) -> Tuple[int, ...]:
        """Get VArray virtual grid."""
        return self.schema.vgrid  # type: ignore[return-value]

    @property
    def arrays_shape(self) -> Tuple[int, ...]:
        """Get inner arrays shape."""
        return self.schema.arrays_shape

    @property
    def as_dict(self) -> dict:
        """Serialize self attributes into dict."""
        d = super().as_dict
        d["vgrid"] = self.vgrid
        return d

    def delete(self) -> None:
        """Delete array from storage."""
        self.__array_adapter.delete_all_by_vid(self.id, self.__collection)
        self._adapter.delete(self)
        self.logger.debug(f"VArray {self.id} is deleted")

    @property
    def schema(self) -> Optional[VArraySchema]:
        """Get array schema."""
        return self.__collection.varray_schema
