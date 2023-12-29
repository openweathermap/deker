# DEKER™

![image](docs/deker/images/logo_50.png)

[![PyPI version shields.io](https://img.shields.io/pypi/v/deker.svg?color=0)](https://pypi.python.org/pypi/deker/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/deker.svg)](https://pypi.python.org/pypi/deker/) 
[![GitHub license](https://badgen.net/github/license/openweathermap/deker)](https://github.com/openweathermap/deker/blob/main/LICENSE)
[![codecov](https://codecov.io/gh/openweathermap/deker/branch/main/graph/badge.svg?token=Z040BQWIOR)](https://codecov.io/gh/openweathermap/deker)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

DEKER™ is pure Python implementation of petabyte-scale highly parallel data storage engine for
multidimensional arrays.

DEKER™ name comes from term *dekeract*, the [10-cube](https://en.wikipedia.org/wiki/10-cube).

DEKER™ was made with the following major goals in mind:

* provide intuitive interface for storing and accessing **huge data arrays**
* support **arbitrary number of data dimensions**
* be **thread and process safe** and as **lean on RAM** use as possible

DEKER™ empowers users to store and access a wide range of data types, virtually anything that can be
represented as arrays, like **geospacial data**, **satellite images**, **machine learning models**,
**sensors data**, graphs, key-value pairs, tabular data, and more.

DEKER™ does not limit your data complexity and size: it supports virtually unlimited number of data
dimensions and provides under the hood mechanisms to **partition** huge amounts of data for
**scalability**.

## Features

* **Open source** under GPL 3.0
* Scalable storage of huge virtual arrays via **tiling**
* **Parallel processing** of virtual array tiles
* Own **locking** mechanism enabling virtual arrays parallel read and write
* Array level **metadata attributes**
* **Fancy data slicing** using timestamps and named labels
* Support for industry standard [NumPy](https://numpy.org/doc/stable/), [Xarray](https://docs.xarray.dev/en/stable/)
* Storage level data **compression and chunking** (via HDF5)

## Code and Documentation

Open source implementation of DEKER™ storage engine is published at

* https://github.com/openweathermap/deker

API documentation and tutorials for the current release could be found at

* https://docs.deker.io

## Quick Start

### Dependencies

Minimal Python version for DEKER™ is 3.9.

DEKER™ depends on the following third-party packages:

* `numpy` >= 1.18
* `attrs` >= 23.1.0
* `tqdm` >= 4.64.1
* `psutil` >= 5.9.5
* `h5py` >= 3.8.0
* `hdf5plugin` >= 4.0.1

Also please not that for flexibility few internal DEKER™ components are published as separate
packages:

* [`deker-local-adapters`](https://github.com/openweathermap/deker-local-adapters)
* [`deker-server-adapters`](https://github.com/openweathermap/deker-server-adapters)
* [`deker-tools`](https://github.com/openweathermap/deker-tools)

### Install

To install DEKER™ run:

   ```bash
   pip install deker
   ```
Please refer to documentation for advanced topics such as running on Apple silicone or using Xarray
with DEKER™ API.

### First Steps

Now you can write simple script to jump into DEKER™ development:

```python
from deker import Client, ArraySchema, DimensionSchema, TimeDimensionSchema
from datetime import datetime, timedelta, timezone
import numpy as np

# Where all data will be kept
DEKER_URI = "file:///tmp/deker"

# Define 3-dimensional schema with to numeric and one time dimension
dimensions = [
   DimensionSchema(name="y", size=128),
   DimensionSchema(name="x", size=128),
   TimeDimensionSchema(
      name="forecast_dt",
      size=128,
      start_value=datetime.now(timezone.utc),
      step=timedelta(3),
   )
]

# Define array schema with float dtype and dimensions
array_schema = ArraySchema(dtype=float, dimensions=dimensions)

# Instantiate client using context manager
with Client(DEKER_URI) as client:
   # Create collection
   collection = client.create_collection("my_collection", array_schema)
   
   # Create array
   array = collection.create()
   
   # Write some data
   array[:].update(np.ones(shape=array.shape))
   
   # And read the data back
   data = array[:].read()
```
