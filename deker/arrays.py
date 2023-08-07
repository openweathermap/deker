# deker - multidimensional arrays storage engine
# Copyright (C) 2023  OpenWeather
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from typing import TYPE_CHECKING, Optional, Tuple

from deker.ABC.base_array import BaseArray
from deker.schemas import ArraySchema, VArraySchema


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.collection import Collection


class Array(BaseArray):
    """``Array`` is an abstract wrapper over final low-level arrays containing data.

    ``Array`` is created by collection. It is a ``lazy`` object that does not interact with its data itself,
    but it possesses all information about the array. Interaction with data is performed by a ``Subset`` object,
    which is produced by ``Array.__getitem__()`` method.

    Properties
    ----------
    - ``as_dict``: serializes main information about array into dictionary, prepared for JSON
    - ``collection``: returns the name of ``Collection`` to which the ``Array`` is bound
    - ``dimensions``: returns a tuple of ``Array's`` dimensions as objects
    - ``dtype``: returns type of the ``Array`` data
    - ``id``: returns ``Array's`` id
    - ``schema``: returns ``Array's`` low-level schema
    - ``shape``: returns ``Array's`` shape as a tuple of dimension sizes
    - ``named_shape``: returns ``Array's`` shape as a tuple of dimension names bound to their sizes

    Attributes
    ~~~~~~~~~~
    According to the schema, arrays may have or may not have primary and/or custom attributes.
    Attributes are stored in low-level array's metadata. Primary attributes are immutable and ordered,
    custom attributes are mutable and unordered.

    - ``primary_attributes``: Primary attributes are used for ``Arrays`` filtering. It is an OrderedDict.
      If collection schema contains attributes schema with some of them defined as `primary`, you obtain
      a possibility to quickly find all ``Arrays`` which can have a certain attribute or meet same
      conditions based on primary attributes values.

      E.g., if we refer to the example above and store the information about arrays' data heights into
      primary attribute, we can create a filter ``{"height": 10}`` and find the ``Array``, which data
      was calculated at height equal to 10 meters.

    - ``custom_attributes``: Custom attributes are used to keep some unique information about the array
      or about its data. E.g. you store some weather data, which is calculated at different heights.
      If it is not important for filtering you can keep the information about certain heights in arrays'
      custom attribute, named "height". In this case you won't be able to search by this attribute,
      but still you'll be able to use this information in your calculations, checking it array by array.
      Anytime you can set this attribute to None for any array as it is not a global setting.
      But you are not able to change dtype of custom attributes. In other words you cannot provide attribute's
      dtype as ``int`` and set there any ``float`` or string or whatever.

    API Methods
    ------------
    - ``read_meta``: reads the array's metadata from storage
    - ``update_custom_attributes``: updates ``Array's`` custom attributes values
    - ``delete``: deletes ``Array`` from the storage with all its data and metadata
    - ``__getitem__``: returns a ``Subset`` instance bound to the ``Array``
    - ``__repr__``: ordinary behaviour
    - ``__str__``: ordinary behaviour

    :param collection: instance of the ``Collection``, to which the ``Array`` is bound
    :param adapter: ``Array`` adapter instance
    :param id_: array unique uuid string
    :param primary_attributes: any primary attributes keyword mapping
    :param custom_attributes: any custom attributes keyword mapping
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
        """Get id of the ``VArray`` to which the ``Array`` is bound."""
        if self.__collection.varray_schema:
            return self.primary_attributes.get("vid")

    @property
    def _v_position(self) -> Optional[Tuple[int, ...]]:
        """Get ``Array`` position in ``VArray``."""
        if self.__collection.varray_schema:
            vpos = self.primary_attributes.get("v_position")
            return tuple(vpos) if vpos else vpos

    @property
    def schema(self) -> ArraySchema:
        """Return ``ArraySchema`` of the Array's Collection."""
        return self.__collection.array_schema


class VArray(BaseArray):
    """Virtual array or ``VArray`` is an abstract virtual array that consists of ordinary ``Arrays``.

    ``VArray`` is an "array of arrays" or an "image of pixels". If we consider ``VArray`` as an image -
    it is split by virtual grid into tiles. In this case, each tile is an ordinary ``Array``.
    Meanwhile, for user ``VArray`` acts as an ordinary ``Array``.

    ``VArray`` is created by ``Collection``. It is a ``lazy`` object that does not interact with its data itself,
    but it possesses all information about the virtual array (and even a bit more). Interaction with data
    is performed by a VSubset object, which is produced by ``VArray.__getitem__()`` method.

    Properties
    -----------
    - ``as_dict``: serializes main information about ``VArray`` into dictionary, prepared for JSON
    - ``arrays_shape``: returns common shape of all the ``Arrays`` bound to the ``VArray``
    - ``collection``: returns the name of collection to which the ``VArray`` is bound
    - ``dimensions``: returns a tuple of ``VArray's`` dimensions as objects
    - ``dtype``: returns type of the ``VArray`` data
    - ``id``: returns virtual ``VArray's`` id
    - ``schema``: returns ``VArray's`` low-level schema
    - ``shape``: returns ``VArray's`` shape as a tuple of dimension sizes
    - ``named_shape``: returns ``VArray's`` shape as a tuple of dimension names bound to their sizes
    - ``vgrid``: returns virtual grid (a tuple of integers) by which ``VArray`` is split into ``Arrays``

    Attributes
    ~~~~~~~~~~
    According to the schema, ``VArrays`` may have or may not have primary attributes and/or custom attributes.
    Attributes are stored in ``VArray's`` metadata. Primary attributes are immutable and ordered,
    custom attributes are mutable and unordered.

    - ``primary_attributes``: Primary attributes are used for ``VArrays`` filtering. It is an OrderedDict.
      If collection schema contains attributes schema with some of them as `primary`, you obtain
      a possibility to quickly find all VArrays which can have a certain attribute or meet the same
      conditions based on primary attributes values.

      E.g., if we refer to the example above and store the information about arrays' data heights into
      primary attribute, we can create a filter ``{"height": 10}`` and find the ``VArray``, which data
      was calculated at heights equal to 10 meters.

    - ``custom_attributes``: Custom attributes are used to keep some unique information about the array
      or about its data. E.g., you store some weather data, which is calculated at different heights.
      If it is not important for filtering, you can keep the information about certain heights in arrays'
      custom attribute, named "height". In this case, you won't be able to search by this attribute,
      but still you'll be able to use this information in your calculations, checking it array by array.
      Anytime you can set this attribute to None for any array as it is not a global setting.
      But you are not able to change dtype of custom attributes. In other words, you cannot provide attribute's
      dtype as `int` and set there any `float` or string or whatever.

    API Methods
    -----------
    - ``read_meta``: reads the array's metadata from storage
    - ``update_custom_attributes``: updates ``VArray's`` custom attributes values
    - ``delete``: deletes ``VArray`` from the storage with all its data and metadata
    - ``__getitem__``: returns a ``Subset`` instance bound to the ``VArray``
    - ``__repr__``: ordinary behaviour
    - ``__str__``: ordinary behaviour

    :param collection: instance of the ``Collection``, to which the ``VArray`` is bound
    :param adapter: ``VArray`` adapter instance
    :param array_adapter: ``Array`` adapter instance
    :param id_: ``VArray`` unique uuid string
    :param primary_attributes: any primary attribute keyword mapping
    :param custom_attributes: any custom attributes keyword mapping
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
        """Get ``VArray`` virtual grid."""
        return self.schema.vgrid  # type: ignore[return-value]

    @property
    def arrays_shape(self) -> Tuple[int, ...]:
        """Get the shape of inner ``Arrays``."""
        return self.schema.arrays_shape  # type: ignore[return-value]

    @property
    def as_dict(self) -> dict:
        """Serialize self into dict."""
        d = super().as_dict
        d["vgrid"] = self.vgrid
        return d

    def delete(self) -> None:
        """Delete ``VArray`` from storage."""
        self.__array_adapter.delete_all_by_vid(self.id, self.__collection)
        self._adapter.delete(self)
        self.logger.debug(f"VArray {self.id} is deleted")

    @property
    def schema(self) -> Optional[VArraySchema]:
        """Return ``VArraySchema`` of the VArray's Collection."""
        return self.__collection.varray_schema
