***********
Data Access
***********

Collections
===========


Retrieving Collections
----------------------

Retrieving collections is ``Client`` responsibility as well as their creation. In the previous
chapter we created ``Collection`` named ``"weather"``. Now we are going to get it::

   from deker import Client, Collection

   with Client("file:///tmp/deker") as client:
      collection: Collection = client.get_collection("weather")

   print(collection)  # weather

If you have several collections on the same storage, you can iterate them with the ``Client``::

   with Client("file:///tmp/deker") as client:
      for collection in client:
         print(collection)

For type checkers ``Collection`` is generic over the kind of arrays it holds:
``create_collection`` with an ``ArraySchema`` gives a ``Collection[Array]``, with a
``VArraySchema`` a ``Collection[VArray]``, and ``create``, ``filter`` and iteration follow that
type. A collection read from the storage can be either, so ``get_collection`` returns
``Collection[Array] | Collection[VArray] | None``; check ``varray_schema`` to tell::

   with Client("file:///tmp/deker") as client:
      collection = client.create_collection("winds", VArraySchema(...))  # Collection[VArray]
      varray = collection.create({"model": "gfs"})                         # VArray
      vsubset = varray[0:10]                                               # VSubset

      stored = client.get_collection("weather")
      if stored is not None and stored.varray_schema is None:
         arrays: Collection[Array] = stored

``Collection`` object has several useful properties and methods for self-managing::

   with Client("file:///tmp/deker") as client:
      collection: Collection = client.get_collection("weather")

      print(collection.name)
      print(collection.array_schema)   # returns schema of Array
      print(collection.varray_schema)  # returns schema of VArray if applicable, else None
      print(collection.path)           # returns physical storage path of the Collection
      print(collection.as_dict)        # serializes main information about Collection into
                                       # dictionary
      collection.clear()               # removes all the Array and/or VArray objects from the
                                       # storage, but retains the Collection metadata
      collection.delete()              # removes all the Array and/or VArray and the Collection
                                       # metadata from the storage


Managers
--------

``Collection`` object has 3 kinds of managers to work with its contents:

   1. ``default`` (or ``DataManager``) is ``Collection`` itself
   2. ``Collection.arrays`` (or ``ArraysManager``) is a manager responsible for ``Array``
   3. ``Collection.varrays`` (or ``VArraysManager``) is a manager responsible for ``VArray``
      (unavailable in ``Array`` collections).

These managers are mixed with ``FilteredManager`` object and are responsible for creation and
filtering of the correspondent contents. All of them have the same interface. The default manager
is a preferred one. Having information about the ``Collection`` main schema, the default manager
decides what to create or to filter. If you have a ``VArray`` collection, it will create or filter
``VArray`` objects, if your collection is made of ``Array`` it will create or filter ``Array``. The
two others are made for direct filtering of ``Array`` or ``VArray``.

Normally, you need the default one, and although the two others are public, we will not describe
them in this documentation.


Array Creation
--------------

Let's create a first Array::

   from datetime import datetime
   from deker import Array, Client, Collection

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.create({"dt": datetime(2023, 1, 1, 0)})
       print(array)

.. note::
   Let's assume that hereinafter all the ``datetime`` objects, including timestamps and ISO 8601
   strings are in **UTC timezone**.

As you remember, our schema contains a ``TimeDimensionSchema`` and a **primary** attribute schema.
``TimeDimensionSchema.start_value`` was indicated as a reference to the ``AttributeSchema.name``,
what allowed you to set an individual time start point for each Array. That's why we passed
``{"dt": datetime(2023, 1, 1, 0)}`` to the method of creation, nevertheless if the attribute was
defined as ``primary`` or ``custom``. Now our ``Array`` knows the day and the hour when its data
time series starts.

If some other primary attributes were defined, values for them should have been included in this
dictionary.

If no attributes are defined in the schema, the method shall be called without parameters:
``collection.create()``.

When an ``Array`` or a ``VArray`` is created, it has a unique ``id`` which is a UUID string.
``Array`` and ``VArray`` IDs are generated automatically by different algorithms. So the
probability to get two same IDs tends to zero.

Fine, we have our first ``Array`` in the ``Collection``. Do we have any changes in our storage?
Yes, we do. If you list it with::

   ls -lh /tmp/deker/collections/weather

You will find out that there are two directories named ``array_data`` and ``array_symlinks`` and a
file with the ``Collection`` metadata ``weather.json``.

Listing these inner directories will tell you that you have an ``.hdf5`` file with the ``Array``
UUID in its name. At the moment this file is almost empty. It contains just the ``Array`` metadata,
as we have not yet inserted any data in it. But it is created and ready to be used.

Thus, we can create all the ``Array`` objects in advance without filling them in with any data and
retrieve them when we need. Let's prepare our database for January 2023::

   from datetime import datetime, timedelta
   from deker import Array, Client, Collection

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")

       for day in range(30):
           start_point = datetime(2023, 1, 2, 0) + timedelta(days=day)
           collection.create({"dt": start_point})

``Collection`` is an iterator, so we can get all its contents item by item::

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       for array in collection:
          print(array)

.. note::
   Everything, mentioned above in this section, is applicable to ``VArray`` as well, except that
   ``VArray`` collection path will contain two more directories: ``varray_data`` and
   ``varray_symlinks``.


Arrays Filtering
----------------

If we need to get a certain ``Array`` from the collection, we shall filter it out. As previously
stated, **primary** attributes allow you to find a certain ``Array`` or ``VArray`` in the
``Collection``. If no primary attribute is defined, you need either to know its ``id`` or to
iterate the ``Collection`` in order to find a particular ``Array`` or ``VArray`` until you get the
right one.

.. attention::
   It is highly recommended to define at least one **primary** attribute in every schema.

So you have two options how to filter an ``Array`` or ``VArray`` in a ``Collection``:

   1. By ``id``
   2. By primary attributes

For example, we saved an ``id`` of some ``Array`` to a variable, let's create a filter::

   from deker import Array, Client, Collection
   from deker.managers import FilteredManager

   id = "9d7b32ee-d51e-5a0b-b2d9-9a654cb1991d"

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       filter: FilteredManager = collection.filter({"id": id})

This ``filter`` is an instance of ``FilteredManager`` object, which is also lazy. It keeps the
parameters for filtering, but no job has been done yet.

.. attention::
   There is no any query language or conditional matching for now, only strict matching is
   available.

The ``FilteredManager`` provides final methods for invocation of the filtered objects:

   * ``first()``
   * ``last()``

Since only strict matching is available, both of them will return the same. They are stubs for
the query language development.

Now let's filter some ``Array`` by the primary attribute::

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")

       filter_1: FilteredManager = collection.filter({"dt": datetime(2023, 1, 3, 0)})
       filter_2: FilteredManager = collection.filter({"dt": datetime(2023, 1, 15, 0).isoformat()})

       array_1: Array = filter_1.first()
       array_2: Array = filter_2.last()
       print(array_1)
       print(array_2)
       assert array_1.id != array_1.id

As you see, attributes of ``datetime.datetime`` type, can be filtered both by ``datetime.datetime``
object as well as by its representation as ISO 8601 string.

.. attention::
   If your collection schema has **several** primary attributes, you must pass filtering values for
   **all** of them!

.. note::
   Everything, mentioned above in this section, is applicable to VArray as well.


Array and VArray
================

As previously stated, both ``Array`` and ``VArray`` objects have the same interface.

Their common **properties** are:

   * ``id``: returns ``Array`` or ``VArray`` ID
   * ``dtype``: returns type of the ``Array`` or ``VArray`` data
   * ``shape``: returns ``Array`` or ``VArray`` shape as a tuple of dimension sizes
   * ``named_shape``: returns ``Array`` or ``VArray`` shape as a tuple of dimension names bound to
     their sizes
   * ``dimensions``: returns a tuple of ``Array`` or ``VArray`` dimensions as objects
   * ``schema``: returns ``Array`` or ``VArray`` low-level schema
   * ``collection``: returns the name of ``Collection`` to which the ``Array`` is bound
   * ``as_dict``: serializes main information about array into dictionary, prepared for JSON
   * ``primary_attributes``: returns an ``OrderedDict`` of ``Array`` or ``VArray`` **primary**
     attributes
   * ``custom_attributes``: returns a ``dict`` of ``Array`` or ``VArray`` **custom** attributes

``VArray`` has two extra properties:

   * ``arrays_shape``: returns common shape of all the ``Array`` bound to the ``VArray``
   * ``vgrid``:  returns virtual grid (a tuple of integers) by which ``VArray`` is split into
     ``Array``

Their common common methods are:

   * ``read_meta()``: reads the ``Array`` or ``VArray`` metadata from storage
   * ``update_custom_attributes()``: updates ``Array`` or ``VArray`` custom attributes values
   * ``delete()``: deletes ``Array`` or ``VArray`` from the storage with all its data and metadata
   * ``__getitem__()``: creates ``Subset`` from ``Array`` or ``VSubset`` from ``VArray``


Updating Custom Attributes
--------------------------

Updating custom attributes is quite simple. As you remember, our schema contains one named ``tm``
(timestamp) with ``int`` data type, and we have never defined its value. It means, that it is set
to ``None`` in each ``Array``. Let's check it and update them everywhere::

   from deker import Array, Client, Collection
   from deker.managers import FilteredManager

   with Client("file:///tmp/deker") as client:
      collection: Collection = client.get_collection("weather")
      for array in collection:
         print(array.custom_attributes)  # {'tm': None}

         # type shall be `int`
         custom_attribute_value = int(array.primary_attributes["dt"].timestamp()))
         array.update_custom_attributes({'tm': custom_attribute_value})

         print(array.custom_attributes)

If there are many custom attributes and you want to update just one or several of them - no
problem. Just pass a dictionary with values for the attributes you need to update. All the others
will not be harmed and will keep their values.


Fancy Slicing
-------------

It is our privilege and pleasure to introduce the **fancy slicing** of your data!

We consider the ``__getitem__()`` method to be one of our pearls.

Usually, you use integers for native Python and NumPy indexing and ``start``, ``stop`` and ``step``
slicing parameters::

   import numpy as np

   python_seq = range(10)
   np_seq = np.random.random((3, 4, 5))

   print(python_seq[1], python_seq[3:], python_seq[3:9:2])
   print(np_seq[2, 3, 4], np_seq[1:,:, 2], np_seq[:2, :, 1:4:2])

.. attention::
   If you are new to NumPy indexing, please, refer to the `official documentation`_

.. _`official documentation`: https://numpy.org/doc/stable/user/basics.indexing.html

DEKER™ allows you to index and slice its ``Array`` and ``VArray`` not only with integers, but with
the ``types`` by which the dimensions are described.

Step
~~~~

The ``step`` of a slice works as it does in NumPy, for both ``Array`` and ``VArray``:
``array[::2]`` takes every other cell, ``array[::-1]`` reverses the axis, ``array[10:2:-2]`` walks
backwards. A ``VArray`` splits the selection over its inner ``Arrays`` itself, whatever the grid::

   varray[::3, 5, ::-1]          # every third row, one column, the last axis reversed
   varray[::2].update(data)      # data has the shape of the selection
   varray[1::2].clear()

Besides integers, a step may be expressed in the values an axis is described with:

* on a ``TimeDimension`` a ``datetime.timedelta`` step shall be a non-zero multiple of the
  dimension ``step``: ``array[start::timedelta(hours=6)]`` with an hourly axis takes every sixth
  hour, ``-timedelta(hours=6)`` walks backwards;
* on a ``Dimension`` with a ``scale`` a ``float`` step shall be a non-zero multiple of the scale
  ``step`` and is counted in the values of the scale: on a latitude axis described as
  ``Scale(start_value=90.0, step=-0.25)`` the step ``-1.0`` walks from the north to the south, that
  is forwards along the axis, and ``1.0`` walks backwards;
* on a ``Dimension`` with ``labels`` only integer steps are valid: ``array[:, "a":"e":2]``.

A zero step, a boolean step, a step of a kind the axis does not accept (a ``float`` on a time axis,
a ``timedelta`` on a scale, a ``str`` anywhere) and a step that is not a multiple of the axis step
raise ``IndexError``. So does a selection that ends up empty, for instance a negative step whose
``start`` lies before its ``stop``.

.. note::
   A ``timedelta`` is a step, never a position: ``start`` and ``stop`` of a time axis take
   ``datetime`` objects, ISO 8601 strings or timestamps.


Start and Stop
~~~~~~~~~~~~~~

As earlier mentioned, if your ``Dimensions`` have an additional description with ``scale`` or
``labels`` you can get rid of indexes calculations and provide your ``scale`` or ``labels`` values
to ``start`` and ``stop`` parameters.

If you have a ``TimeDimension``, you can slice it with ``datetime.datetime`` objects, its ISO 8601
formatted string or timestamps in the type of ``float``.

.. attention::
   Remember, that you shall convert your local timezone to UTC for proper ``TimeDimension`` slicing.

Let's have a closer look::

   from datetime import datetime
   from deker import Array, Client, Collection

   with Client("file:///tmp/deker") as client:
      collection: Collection = client.get_collection("weather")

      array: Array = collection.filter({"dt": datetime(2023, 1, 3, 0)}).first()

      start_dt = datetime(2023, 1, 3, 5)
      end_dt = datetime(2023, 1, 3, 10)

      fancy_subset = array[
         start_dt:end_dt,  # step is timedelta(hours=1)
         -44.0:-45.0,      # y-scale start point is 90.0 and step is -1.0 (90.0 ... -90.0)
         -1.0:1.0,         # x-scale start point is -180.0 and step is 1.0 (-180.0 ... 179.0)
         :"pressure"       # captures just "temperature" and "humidity"
      ]

      # which is equivalent of:
      subset = array[
         5:10,
         134:135,
         179:181,
         :2
      ]

      assert fancy_subset.shape == subset.shape
      assert fancy_subset.bounds == subset.bounds

It is great, if you can keep in mind all the indexes and their mappings, but this feature awesome,
isn't it?! Yes, it is!!!

The values, passed to each dimension's index or slice, are converted to integers, and after that
they are set in the native Python ``slice`` object. A ``tuple`` of such ``slices`` is the final
representation of the bounds which will be applied to your data.

.. attention::
   Fancy index values must **exactly** match your dimension time, ``Scale`` or ``label`` values,
   otherwise, you will get ``IndexError``.

You have not yet approached your data, but you are closer and closer.

Now you have a new object - `Subset`.


Subset and VSubset
==================

``Subset`` and ``VSubset`` are the final lazy objects for the access to your data.

Once created, they contain no data and do not access the storage until you manually invoke one of
their correspondent methods.

.. note::
   If you need to read or write all the data from ``Array`` or ``VArray`` you should create a
   subset with ``[:]`` or ``[...]``.

Both of them also have the same interface. As for the properties, they are:

   * ``shape``: returns shape of the ``Subset`` or ``VSubset``
   * ``bounds``: returns bounds that were applied to ``Array`` or ``VArray``
   * ``dtype``: returns type of queried data
   * ``fill_value``: returns value for empty cells

Let's dive deeper into the methods.

.. note::
   The explanations below are based on the logic, implemented for the ``HDF5`` format.


Read
----

Method ``read()`` gets data from the storage and returns a ``numpy.ndarray`` of the corresponding
``shape`` and ``dtype``. Regarding ``VArray`` data reading, ``VSubset`` will capture the data from
the ``Array``, affected by the passed bounds, arrange it in a single ``numpy.ndarray`` of the
proper ``shape`` and ``dtype`` and return it to you.

If your ``Array`` or ``VArray`` is **empty** - a ``numpy.ndarray`` filled with ``fill_value`` will
be returned for any called ``Subset`` or ``VSubset``::

   import numpy as np
   from datetime import datetime
   from deker import Array, Client, Collection

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.filter({"dt": datetime(2023, 1, 15, 0)}).first()
       subset = array[0, 0, 0]  # get first hour and grid zero-point
       print(subset.read())  # [nan, nan, nan, nan]


Update
------

Method ``update()`` is an **upsert** method, which is responsible for new values **inserting** and
old values **updating**.

The shape of the data, that you pass into this method, shall match the shape of the ``Subset`` or
``VSubset``. It is impossible to insert 10 values into 9 cells. It is also impossible to insert
them into 11 cells, as there are no instructions how to arrange them properly. ::

   import numpy as np
   from datetime import datetime
   from deker import Array, Client, Collection

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.filter({"dt": datetime(2023, 1, 1, 0)}).first()
       subset = array[:]  # captures full array shape

       data = np.random.random(subset.shape)

       subset.update(data)

A scalar is the exception to the shape rule: it fills every cell of the ``Subset`` or ``VSubset``,
whatever its shape, so ``subset.update(0.0)`` zeroes the whole selection of an ``Array`` and of a
``VArray`` alike::

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.filter({"dt": datetime(2023, 1, 1, 0)}).first()
       array[:, :, 0].update(273.15)  # one value in every cell of the selection

Numpy data is written as it is: the ``dtype`` of a ``numpy.ndarray`` or a numpy scalar shall be
exactly the dtype of the ``Array`` or ``VArray`` set by the schema, otherwise
``DekerArrayTypeError`` is raised; numpy types are never converted to one another. Python
numbers (a scalar, a list or a tuple, nested as deep as the selection has dimensions) are
converted to the array dtype, provided every value fits it: an integer dtype takes the values it
holds exactly (no wrapping, no fractions, no ``nan``), a float dtype takes the values it holds up
to rounding (a finite value stays finite), a complex dtype applies the same to both parts. A value
that does not fit raises
``DekerArrayTypeError`` before anything is written::

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.filter({"dt": datetime(2023, 1, 1, 0)}).first()
       subset = array[:]  # captures full array shape

       data = np.random.random(subset.shape).tolist()  # Python list of Python floats

       subset.update(data)  # data will be converted to array.dtype

       array[0, 0, 0].update(300)  # DekerArrayTypeError for a uint8 array: 300 does not fit

If your ``Array`` or ``VArray`` is utterly empty, ``Subset`` or ``VSubset`` will create a
``numpy.ndarray`` of the ``Array`` shape filled with the ``fill_value`` from the ``Collection``
schema and than, using the indicated bounds, it will insert the data provided by you in this array.
Afterwards it will be dumped to the storage. In the scope of ``VArray`` it will work in the same
manner, except that only corresponding affected inner ``Array`` will be created.

If there is some data in your ``Array`` or ``VArray`` and you provide some new values by this
method, the old values in the affected bounds will be substituted with new ones::

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.filter({"dt": datetime(2023, 1, 1, 0)}).first()

       data = np.random.random(array.shape)
       array[:].update(data)

       subset = array[0, 0, 0]  # get first hour and grid zero-point

       print(subset.read())  # a list of 4 random values

       new_values = [0.1, 0.2, 0.3, 0.4]
       subset.update(new_values)  # data will be converted to array.dtype

       print(subset.read())  # [0.1, 0.2, 0.3, 0.4]


Clear
-----

Method ``clear()`` inserts the ``fill_value`` into the affected bounds. If all your ``Array`` or
``VArray`` values are ``fill_value``, it will be concerned empty and the data set will be deleted
from the file. But the file still exists and retains ``Array`` or ``VArray`` metadata::

   import numpy as np
   from datetime import datetime
   from deker import Array, Client, Collection

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.filter({"dt": datetime(2023, 1, 1, 0)}).first()

       data = np.random.random(array.shape)
       array[:].update(data)

       subset = array[0, 0, 0]  # get first hour and grid zero-point

       print(subset.read())  # a list of 4 random values

       new_values = [0.1, 0.2, 0.3, 0.4]
       subset.update(new_values)  # data will be converted to array.dtype
       print(subset.read())  # [0.1, 0.2, 0.3, 0.4]

       subset.clear()
       print(subset.read())  # [nan, nan, nan, nan]

       array[:].clear()
       print(array[:].read()) # a numpy.ndarray full of `nans`


Describe
--------

You may want to check, what part of data you are going to manage.

With ``describe()`` you can get an ``OrderedDict`` with a description of the dimensions parts
affected by ``Subset`` or ``VSubset``. If you provided ``scale`` and/or ``labels`` for your
dimensions, you will get the human-readable description, otherwise you'll get indexes.

So it is highly recommended to describe your dimensions::

   from datetime import datetime
   from deker import Array, Client, Collection
   from pprint import pprint

   with Client("file:///tmp/deker") as client:
      collection: Collection = client.get_collection("weather")
      array: Array = collection.filter({"dt": datetime(2023, 1, 1, 0)}).first()

      pprint(array[0, 0, 0].describe())

      # OrderedDict([('day_hours',
      #             [datetime.datetime(2023, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)]),
      #             ('y', [90.0]),
      #             ('x', [-180.0]),
      #             ('weather', ['temperature', 'humidity', 'pressure', 'wind_speed'])])

      subset = array[datetime(2023, 1, 1, 5):datetime(2023, 1, 1, 10),
                     -44.0:-45.0,
                     -1.0:1.0,
                     :"pressure"]

      pprint(subset.describe())

      #  OrderedDict([('day_hours',
      #               [datetime.datetime(2023, 1, 1, 5, 0, tzinfo=datetime.timezone.utc),
      #                datetime.datetime(2023, 1, 1, 6, 0, tzinfo=datetime.timezone.utc),
      #                datetime.datetime(2023, 1, 1, 7, 0, tzinfo=datetime.timezone.utc),
      #                datetime.datetime(2023, 1, 1, 8, 0, tzinfo=datetime.timezone.utc),
      #                datetime.datetime(2023, 1, 1, 9, 0, tzinfo=datetime.timezone.utc)]),
      #              ('y', [-44.0]),
      #              ('x', [-1.0, 0.0]),
      #              ('weather', ['temperature', 'humidity'])])

.. attention::
   Description is an ``OrderedDict`` object, having in values full ranges of descriptive data for
   ``Subset`` or ``VSubset``. If you keep this description in memory, your memory will be lowered
   by its size.


Read Xarray
-----------

.. warning::
   ``xarray`` package is not in the list of the DEKER™ default dependencies. Please, refer to the
   Installation_ chapter for more details

Xarray_ is a wonderful project, which provides special objects for working with multidimensional
data. Its main principle is *the data shall be described*. We absolutely agree with that.

Method ``read_xarray()`` describes a ``Subset`` or ``VSubset``, reads its contents and converts it
to ``xarray.DataArray`` object.

If you need to convert your data to ``pandas`` objects, or to ``netCDF``, or to ``ZARR`` - use this
method and after it use methods, provided by ``xarray.DataArray``::

   import numpy as np
   from datetime import datetime
   from deker import Array, Client, Collection

   with Client("file:///tmp/deker") as client:
       collection: Collection = client.get_collection("weather")
       array: Array = collection.filter({"dt": datetime(2023, 1, 1, 0)}).first()

       data = np.random.random(array.shape)
       array[:].update(data)

       subset = array[0, 0, 0]  # get first hour and grid zero-point

       x_subset: xarray.DataArray = subset.read_xarray()

       print(dir(x_subset))
       print(type(x_subset.to_dataframe()))
       print(type(x_subset.to_netcdf()))
       print(type(x_subset.to_zarr()))

It provides even more opportunities. Refer to ``xarray.DataArray`` API_ for details .

.. _Installation: installation.html#extra-dependencies
.. _Xarray: https://docs.xarray.dev/en/stable/
.. _API: https://docs.xarray.dev/en/stable/generated/xarray.DataArray.html

Locks
======
DEKER™ is thread and process safe. It uses its own locks for the majority of operations.
DEKER™ locks can be divided into two groups: **read** and **write** locks

**Read locks** can be shared between threads and processes with no risk of data corruption.

**Write locks** are exclusive and are taken for the files with correspondent data content.
Only the process/thread, which has already acquired a write lock, may produce any changes
in the data.

It means that if one process is already writing some data into a ``HDF5`` file (or into
an ``Array``) and some other processes want to read from it or to write some other data
into the same file, they will receive a ``DekerLockError``.

.. note::
   Reading data from an ``Array``, which is locked for writing, is impossible.

Speaking about ``VArray`` it means that several processes are able to update data
in several non-intersecting ``VSubsets``. In case if any updating ``VSubset`` intersects
with another one, the update operation will be rejected for the ``VSubset``, which met
the write lock.

Please note that operation of custom attributes updating also locks Array or VArray files
for writing.

The locks are file locks of the operating system (``flock``) taken on the data files themselves,
so they work for the processes of one machine whatever way they run, in containers sharing
a volume as well. A read holds a shared lock from its start to its end and leaves no file behind:
a reader that dies in the middle of a read releases its lock at once, and a writer never waits for
a dead reader. A writer waits for the readers of the file to leave (``write_lock_timeout``,
checked every ``write_lock_check_interval`` seconds) and then holds the file exclusively.

The only lock files on the disk are the markers a ``VArray`` write leaves next to its inner
``Arrays`` while it writes them, named ``<array id>.hdf5:<pid>:<token>.varraylock``; the token
tells the process apart from the processes of other containers, which may share its PID.
A process that dies while writing a ``VArray`` leaves its markers behind: ``client.clear_locks()``
removes them, and ``client.check_integrity()`` reports them.

A tile of a ``VArray`` that does not exist yet is created by the first writer to reach it, under
a lock on the ``VArray`` itself, so the writers of one fresh ``VArray`` never create two files for
one position: the later writers find the tile and update it. A writer of a ``VArray`` waits for a
tile held by a writer of another process the same way it waits for readers.
