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
# isort: skip_file

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.dimensions import Dimension, TimeDimension
from deker.managers import FilteredManager
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)
from deker.subset import Subset, VSubset
from deker.types.public.classes import Scale
from deker_local_adapters.storage_adapters.hdf5.hdf5_options import (
    HDF5Options,
    HDF5CompressionOpts,
)

__all__ = (
    # deker.adapters.hdf5
    "HDF5CompressionOpts",
    "HDF5Options",
    # deker.arrays
    "Array",
    "VArray",
    # deker.client
    "Client",
    # deker.collection
    "Collection",
    # deker.dimensions
    "Dimension",
    "TimeDimension",
    # deker.schemas
    "ArraySchema",
    "AttributeSchema",
    "DimensionSchema",
    "TimeDimensionSchema",
    "VArraySchema",
    # deker.subset
    "Subset",
    "VSubset",
)
