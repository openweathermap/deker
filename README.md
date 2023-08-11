# Deker
![image](docs/deker/images/logo_50.png)

[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)
[![made-with-sphinx-doc](https://img.shields.io/badge/Made%20with-Sphinx-1f425f.svg)](https://www.sphinx-doc.org/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)

[![PyPI pyversions](https://img.shields.io/pypi/pyversions/deker.svg)](https://pypi.python.org/pypi/deker/)
[![PyPI version shields.io](https://img.shields.io/pypi/v/deker.svg)](https://pypi.python.org/pypi/deker/)
[![GitHub license](https://badgen.net/github/license/openweathermap/deker)](https://github.com/openweathermap/deker/blob/main/LICENSE)  
[![pipeline](https://github.com/openweathermap/deker/actions/workflows/on_release.yml/badge.svg)](https://github.com/openweathermap/deker/actions/workflows/on_release.yml)
[![docs](https://github.com/openweathermap/deker/actions/workflows/on_release.yml/badge.svg)](https://github.com/openweathermap/deker/actions/workflows/on_release.yml)

**Deker** is pure Python implementation of petabyte-scale highly parallel data storage engine for
multidimensional arrays.

Deker name comes from term *dekeract*, the [10-cube](https://en.wikipedia.org/wiki/10-cube).

Deker was made with the following major goals in mind:

   * provide intuitive interface for storing and accessing **huge data arrays**
   * support **arbitrary number of data dimensions**
   * be **thread and process safe** and as **lean on RAM** use as possible

Deker empowers users to store and access a wide range of data types, virtually anything that can be
represented as arrays, like **geospacial data**, **satellite images**, **machine learning models**,
**sensors data**, graphs, key-value pairs, tabular data, and more.

Deker does not limit your data complexity and size: it supports virtually unlimited number of data
dimensions and provides under the hood mechanisms to **partition** huge amounts of data for
**scalability**.

## Features

* **Open source** under GPL 3.0
* Scalable storage of huge virtual arrays via **tiling**
* **Parallel processing** of virtual array tiles
* Own **locking** mechanism enabling arrays parallel read and write
* Array level **metadata attributes**
* **Fancy data slicing** using timestamps and named labels
* Support for industry standard [NumPy](https://numpy.org/doc/stable/), [pandas](https://pandas.pydata.org/docs/) and 
[Xarray](https://docs.xarray.dev/en/stable/)
* Storage level data **compression and chunking** (via HDF5)


## Code and Documentation

Open source implementation of Deker storage engine is published at

  * https://github.com/openweathermap/deker

API documentation and tutorials for the current release could be found at

  * https://docs.deker.io


## Installation

> **Apple Silicon (M series CPU)**  
> 
> Deker uses NumPy, and some NumPy types are unsupported on current NumPy arm64 version. So if you
> want to use Deker library on Apple Silicon (M series CPU), you have to install x86_64 version of
> Python using 
> [Rosetta](https://developer.apple.com/documentation/apple-silicon/about-the-rosetta-translation-environment) 
> x86_64 to arm64 dynamic binary translator.  
> 
> You may use the following [guide](https://sixty-north.com/blog/pyenv-apple-silicon.html) to install
> x86_64 version of Python an then switch to that version in your Deker project using ``pyenv`` and
> install Deker package as usual.


### Deker
```
pip install deker
```


If you wish to convert your data into [Xarray](https://docs.xarray.dev/en/stable/getting-started-guide/installing.html) 
or pandas *(or even some other)* objects:

```
pip install deker[xarray]
```

Or you can install them separately::
```
pip install deker
pip install xarray
```

### Interactive Shell

``deker-shell`` is an interactive environment that enables you to manage and access Deker storage
in a convenient way. It requires ``deker`` package to be installed manually before use as described
above.

To install interactive shell package
```
pip install deker deker-shell
```

### Deker Tools

``deker-tools`` is an out-of-box battery which provides several useful tools and utilities to work
with Deker data. You may find this package useful in projects, even if they are not related to
Deker.

To install Deker tools package:
```
pip install deker-tools
```


## Usage
```python
from deker import Client, ArraySchema, DimensionSchema, TimeDimensionSchema
from datetime import datetime, timedelta, timezone
import numpy as np

# Where all data will be kept
DEKER_URI = "file:///tmp/deker"

# Define dimension schemas
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

# Define array schema with dtype and dimensions
array_schema = ArraySchema(dtype=float, dimensions=dimensions)

# Instantiate client using context manager
with Client(DEKER_URI) as client:
   # Create collection
   collection = client.create_collection("FIRST COLLECTION", array_schema)
   
   # Create array (You can pass primary and/or custom attributes defined in schema here)
   array = collection.create()
   
   # Write data
   array[:].update(np.ones(shape=array.shape))
   
   # Read data
   data = array[:].read()
```
