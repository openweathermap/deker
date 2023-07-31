# Deker
![image](docs/deker/images/logo_50.png)

**Deker** - is a pure-Python NoSQL database framework, which provides storing multidimensional spatial raster
numeric data and its further simple, fast and comfortable accessing and managing.

It perfectly fits for a vast variety of data:

- geospatial data (cartography, geodesy, meteorology, …, even outer space),
- images,
- video,
- audio,
- biomedicine,
- genomics,
- finance,
- ML,
- ...

and many others – everything that may be represented and stored as a pack of numbers.

Deker is not really limited by a number of dimensions – it’s up to you to decide how complicated your structures
shall be and how many dimensions you use _(our current goal is 5, at the moment)_.

Actually, it is a scalable high-level wrapper over different file formats.  
At the moment Deker supports just ``HDF5``, but we’ll be glad to accept PRs with new storage adapters:
  ``TIFF``, ``NetCDF``, ``ZARR``, … Any format you like and need, even ``JSON`` or ``TXT``.

Deker uses [NumPy](https://numpy.org/doc/stable/) structures and provides an additional support for 
[Xarray](https://docs.xarray.dev/en/stable/), [pandas](https://pandas.pydata.org/docs/) and others.

## Documentation
📖 Check out our [documentation]() for more details!
✍️ Visit the [contribution guide]()


## Installation

### Required dependencies

    python >= 3.9

Deker dependencies are external:

- numpy>=1.18
- attrs>=23.1.0
- tqdm>=4.64.1
- psutil>=5.9.5

and internal:

- deker-tools
- deker-local-adapters
   * h5py>=3.8.0
   * hdf5plugin>=4.0.1

Deker comes with the above mentioned dependencies out of the box:
   ```bash
   pip install deker
   ```

or:
   ```bash
   python -m pip install deker
   ```

Extra dependencies
------------------
- xarray>=2023.5.0

If you wish to convert your data into Xarray 
([xarray installation options](https://docs.xarray.dev/en/stable/getting-started-guide/installing.html)) or pandas 
*(or even some other)* objects:

```bash
pip install deker[xarray]
```

or

```bash
python -m pip install deker[xarray]
```

Or you can install them separately::
```
pip install deker
pip install xarray
```
or 
```bash
python -m pip install deker
python -m pip install xarray
```
### ARM architecture family
Deker uses NumPy, and some NumPy types are unsupported on current NumPy ARM version.  
If you want to run Deker library on your Mac with M1+ chip inside, you need to install **python x86_64** with 
[Rosetta](https://support.apple.com/en-us/HT211861).

Use this [guide](https://towardsdatascience.com/how-to-use-manage-multiple-python-versions-on-an-apple-silicon-m1-mac-d69ee6ed0250) 
or follow next steps:

1. Install Rosetta (ARM -> x86_64 translator): 
   ```bash
   softwareupdate --install-rosetta
   ```
2. Create a Rosetta terminal:  
   2.1. Duplicate your terminal (`apps -> utilities -> right click -> duplicate`) or `install new`.   
   2.2. Click `Get info` on new terminal and set `Open using Rosetta`  

3. Install homebrew: 
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```  
4. Add alias to your `zsh` config file: 
   ```bash
   alias rbrew="arch -x86_64 /usr/local/bin/brew"
   ```  
5. Install python: 
   ```bash
   rbrew install python@3.10
   ```  

After that you can install Deker run `pip install deker`

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
