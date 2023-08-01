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

# nopycln: file
from .base_adapters import BaseCollectionAdapter, BaseStorageAdapter, IArrayAdapter
from .base_array import BaseArray
from .base_collection import BaseCollectionOptions
from .base_dimension import BaseDimension
from .base_managers import BaseAbstractManager, BaseManager
from .base_schemas import BaseArraysSchema, BaseAttributeSchema, BaseDimensionSchema, BaseSchema
from .base_subset import BaseSubset
