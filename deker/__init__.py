# nopycln: file
# isort: skip_file

from deker_local_adapters.storage_adapters.hdf5.hdf5_options import (
    HDF5Options,
    HDF5CompressionOpts,
)

from deker.__version__ import __version__
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

__all__ = (
    "__version__",
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
    # deker.types.classes
)
