.. currentmodule:: deker

*************
Fine tuning
*************

This chapter is dedicated to advanced settings and features provided by Deker.

Client
=========

In addition to the URI parameter ``Client`` accepts several options, that you may want or need to tune.
All of them shall be explicitly passed as ``keyword`` parameters, none of them is positional.

executor
---------
Deker creates its own ``ThreadPoolExecutor`` instance for working with ``VArrays``. By default, this parameter
is ``None``. You may want to use your own ``ThreadPoolExecutor`` (or some **custom** executor, based on
``ThreadPoolExecutor``) instance. In this case Deker will use the passed one::

   from deker import Client

   client = Client(uri, executor=<your_executor_instance>)

.. note:: None executor is initialized and used if you work with a ``Collection`` of ``Arrays``.
   The executor, passed by you, will be ignored.

.. attention:: When Client is closed your executor will not be shutdown, you shall do it manually.

.. caution:: Be aware of the probable threads stuck when using this feature! It may happen because your
   executor may simultaneously manage some outer tasks and may have insufficient number of free threads to resolve
   all the stuff.

workers
---------
This is a parameter for the native Deker executor mentioned above.

By default, it is ``None`` and in this case Deker uses the maximum number of threads from the formula,
provided by `Python 3.9 documentation`_ : ``cpu_count() + 4``

You may increase or reduce it, if you need::


   from deker import Client

   client = Client(uri, workers=8)

.. _Python 3.9 documentation: https://docs.python.org/3.9/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor

write_lock_timeout
--------------------
Deker uses its own file-locks for different operations, one of which is **writing**.
With ``write_lock_timeout`` you can modify an amount of seconds during which a parallel writing process waits
for the release of the locked file::

   from deker import Client

   client = Client(uri, write_lock_timeout=120)

The default is ``60`` seconds. The units are immutable and only ``integers`` are accepted.


write_lock_check_interval
--------------------------
While the parallel writing process waits for the lock release, it sleeps for some time and then checks
the state of the lock. You can adjust its sleeping time in seconds::

   from deker import Client

   client = Client(uri, write_lock_check_interval=5)

The default is ``1`` second. The units are immutable and only ``integers`` are accepted.

loglevel
---------
All the Deker objects (including private ones) have their own loggers. They are bound by the common logging
level, which defaults to ``"ERROR"``. If you need, you may change it at ``Client's`` start::

   from deker import Client

   client = Client(uri, loglevel="INFO")

If you need to change it on the fly, you may use the following function::

   from deker.log import set_logging_level

   <...your code...>

   set_logging_level("INFO")  # now Deker logs starting from "INFO" level

   <...your code...>

memory_limit
--------------
This parameter is used for the early runtime break in case of potential memory overflow.

Deker operates big amounts of data, and you may be unaware that your machine will probably run out of memory.
For example, NumPy shall raise ``_ArrayMemoryError`` if you do something like this::

   >>> import numpy as np

   >>> np.random.random((100000, 100000))
   # numpy.core._exceptions._ArrayMemoryError: Unable to allocate 74.5 GiB
   # for an array with shape (100000, 100000) and data type float64

Insofar as Deker is lazy, you shall be warned about such problems beforehand. For that purpose, Deker checks the memory
limits when it is creating:

   - a ``Collection``
   - a ``Subset`` or ``VSubset``
   - an ``xarray.DataArray`` from a ``Subset`` or a ``VSubset``

By default Deker is **limited** to your **total** ``RAM`` size and your **total** ``swap`` size. For example, you have
16 Gb of RAM and 2 Gb of swap. Thus, Deker is limited to 18 Gb of memory by default. But usually a machine is already
using some parts of these memories for other processes. So your current available free memory is always lower than the
total one.

Deker compares its limits with your current available free memory (RAM + swap) and chooses the minimal one of them.
Than it compares the result with the requested values' shape and dtype. In case if your request requires too much
memory or you are trying to create a ``Collection`` with a schema, which may cause a memory overflow in future,
``DekerMemoryError`` will be immediately raised.

You can lower the default value by passing a certain number of ``bytes`` or by passing a human representation of
kilobytes, megabytes or gigabytes, for example: ``"1024K"``, ``"512M"``, ``"8G"``::

   from deker import Client

   client = Client(uri, memory_limit="4G")  # 4 gigabytes
   client = Client(uri, memory_limit=4096)  # 4096 bytes

Only integers are acceptable for both of bytes and human representation. Capitalization of units postfix is ignored:
``"1024k"``, ``"512m"``, ``"8g"`` will work.

.. note::
   You definitely may want to use it in **Docker**.

   If you set a memory limit to your container, you'd better limit Deker to the same value. Otherwise your container
   may be killed because of memory overflow.

HDF5 options
==============
.. _`HDF5 official documentation`: https://portal.hdfgroup.org/display/HDF5/HDF5

.. attention:: If you are new to ``HDF5``, please, refer to the `HDF5 official documentation`_

Very briefly, ``HDF5`` is a data model, library, and file format for storing and managing data. It supports an unlimited
variety of datatypes, and is designed for flexible and efficient I/O and for high volume and complex data. This format
offers a big number of special tuning options. We will talk about ``chunks`` and data ``compression``.

Deker ``deker-local-adapters`` plugin has its default implementation of working with this format. It depends on two
packages: `h5py`_ and `hdf5plugin`_ which provide a Python interface for HDF5 binaries and a pack of compression
filters.

.. _h5py: https://docs.h5py.org/en/stable/
.. _hdf5plugin: http://www.silx.org/doc/hdf5plugin/latest/

Deker applies chunks and compression options to all of the files within one collection. As long as you do not interact
directly with the files and low-level interfaces, Deker provides special types for these options usage. Your
settings are stored in the collection metadata. When you invoke a ``Collection``, they are recovered and ready to be
applied to your data. But they have to make a trip from the collection metadata to the final data, that's why we need
``HDF5Options`` and ``HDF5CompressionOpts`` objects.

.. note:: Chunks and compression options are applied to your dataset within HDF5-file when the data is inserted or
   updated. When reading, HDF5-file already knows how to manage its chunked and/or compressed contents properly.

First of all, let's prepare a collection schema once again::

    from datetime import datetime, timedelta
    from deker import (  # in order of appearance
        TimeDimensionSchema,
        DimensionSchema,
        Scale,
        AttributeSchema,
        ArraySchema,
        Client,
        Collection
    )

    dimensions = [
        TimeDimensionSchema(
            name="day_hours",
            size=24,
            start_value="$dt",
            step=timedelta(hours=1)
        ),
        DimensionSchema(
            name="y",
            size=181,
            scale=Scale(start_value=90.0, step=-1.0, name="lat")
        ),
        DimensionSchema(
            name="x",
            size=360,
            scale=Scale(start_value=-180.0, step=1.0, name="lon")
        ),
        DimensionSchema(
            name="weather",
            size=4,
            labels=["temperature", "humidity", "pressure", "wind_speed"]
        ),
    ]
    attributes = [
        AttributeSchema(name="dt", dtype=datetime, primary=True),
        AttributeSchema(name="tm", dtype=int, primary=False),
    ]

    array_schema = ArraySchema(
        dimensions=dimensions,
        attributes=attributes,
        dtype=float,  # will be converted and saved as numpy.float64
        # fill_value is not passed - will be numpy.nan
    )

Chunks
--------
Correct data chunking may increase your performance. It makes your data split in smaller equal pieces.
When you read data from a chunk, HDF5-file opens and caches it. The next reading of the same pattern
will be much faster as it will be captured not from the storage, but from the cache.

A HDF5-file may have ``no chunks`` options or be chunked either ``manually`` or ``automatically``.

.. admonition:: Hint

   Study `HDF5 chunking manual`_ to understand **chunks** better.

.. _HDF5 chunking manual: https://portal.hdfgroup.org/display/HDF5/Chunking+in+HDF5

Deker allows you to use all the 3 options.

Chunks options are set to ``None`` by default.

::

   from deker import Client

   with Client("file:///tmp/deker") as client:
      client.create_collection("weather", array_schema)

When you create an ``Array``, its file is one big chunk.

If you set chunks to ``True``, HDF5-file will automatically determine a chunk size with its own algorythm, basing
on the shape of your ``Array``::

   from deker import Client, HDF5Options

   with Client("file:///tmp/deker") as client:
      client.create_collection(
          "weather_chunked_automatically",
          array_schema,
          HDF5Options(chunks=True)
   )

You will never know the final chunk size, but be sure that your data is chunked now.

If you need to adjust it, you may set it manually. It shall be a tuple of integers. The size of the tuple shall
be equal to your ``Array`` shape. Its values shall divide your dimensions without remainders::

   from deker import Client, HDF5Options

   chunks = (1, 181, 36, 4)

   # schema shape is (24, 181, 360, 4)
   # (24, 181, 360, 4) / (1, 181, 36, 4) = (24.0, 1.0, 10.0, 1.0) - no remainders

   with Client("file:///tmp/deker") as client:
      client.create_collection(
          "weather_chunked_manually",
          array_schema,
          HDF5Options(chunks=chunks)
   )

Here we chunked our data into pieces, each of which will contain 1 hour, 181 ``y`` points *(because 181
is a natural number and is divisible only by itself or 1)*, 36 ``x`` points and the full scope of weather layers.
If you need to read some data, which is kept in one or several chunks, the file will not affect other chunks,
but it will open and cache the correspondent ones.

.. admonition:: Hint

   The best way to decide on chunk size is your the most frequently used reading pattern.

Compression
--------------
To prevent a lack of the disc space for your data, you can compress it with different filters, supported by HDF5
and provided by ``h5py`` and ``hdf5plugin`` packages.

There are several default filters, set in ``h5py`` and a pack of the most popular filters, brought by ``hdf5plugin``.

Default filters:
   - ``GZip``
   - ``Lzf``
   - ``SZip``

Custom filters, brought by ``hdf5plugin``:
   - ``Bitshuffle``
   - ``Blosc``
   - ``BZip2``
   - ``FciDecomp``
   - ``LZ4``
   - ``SZ``
   - ``SZ3``
   - ``Zfp``
   - ``Zstd``

.. attention:: The data is compressed chunk by chunk. If you use compression without indicating a chunk size, it will
   be automatically set to `True` and calculated by the inner HDF5 algorythm.

The default filters shall be used as follows::

   from deker import Client, HDF5Options, HDF5CompressionOpts

   with Client("file:///tmp/deker") as client:

      compression=HDF5CompressionOpts(compression="gzip", compression_opts=9),
      options = HDF5Options(compression_opts=compression)
      client.create_collection(
          "weather_chunked_automatically_gzip",
          array_schema,
          collection_options=options
      )

The custom filters shall be instantiated and passed to ``HDF5CompressionOpts`` as a mapping::

   import hdf5plugin
   from deker import Client, HDF5Options, HDF5CompressionOpts

   with Client("file:///tmp/deker") as client:

      compression=HDF5CompressionOpts(**hdf5plugin.Zstd(6)),
      options = HDF5Options(chunks=(1, 181, 36, 4), compression_opts=compression)
      client.create_collection(
          "weather_chunked_manually_zstd",
          array_schema,
          collection_options=options
      )

.. admonition:: Hint

   Dive into **compression options** at `h5py filter pipeline`_, `hdf5plugin docs`_ and `HDF5 compression manual`_.

.. _h5py filter pipeline: https://docs.h5py.org/en/stable/high/dataset.html#filter-pipeline
.. _hdf5plugin docs: http://www.silx.org/doc/hdf5plugin/latest/
.. _HDF5 compression manual: https://portal.hdfgroup.org/display/HDF5/Using+Compression+in+HDF5
