Introduction
============

Deker allows you to define separate storage collections for different kinds of arrays that are
described with own dimensions schemas, stored data type and array level metadata attributes. It
enables you to find and array in collection using it's metadata and then access any part of it
using convenient slicing syntax.

Deker was also built with scalability and flexibility in mind, so all data is stored and accessed
via the separate abstraction level of **storage adapters** which enables flexibility and
scalability in the future. Currenly there is ``deker-local-adapters_`` package that implements
local filesystem storage in HDF5_ format.

.. _HDF5: https://www.h5py.org
.. _deker-local-adapters: https://github.com/openweathermap/deker-local-adapters


Interface Overview
==================

Accessing Data
--------------

Deker APIs are designed to be as simple and user friendly as possible. There are only few
self-explanotary high-level objects that you would need to interact with:

   - :class:`Client <deker.client.Client>`
   - :class:`Collection <deker.collection.Collection>`
   - :class:`Array <deker.arrays.Array>` or :class:`VArray <deker.arrays.VArray>`
   - :class:`Subset <deker.subset.Subset>` or :class:`VSubset <deker.subset.VSubset>`

``Client`` is the first object you start with. It is used for managing and accessing
``Collection`` of ``Array`` or ``VArray`` objects.

``Array`` is a representation of data array stored in a separate physical file.

``VArray`` (or **virtual array**) is an abstraction that represents bigger array that for
scalability reasons is split into multiple separate physical files. This approach in addition to
scalable parallel storage access enables concurrent read and update operations to be performed on
the same logical virtual array object.

You may think about ``VArray`` as of huge image that was split into equally sized tiles using
some regular grid.

You may decide if you want to store arrays or virtual arrays in your collections based purely on
performance and storage scalability considerations, as from the developer point of view there is no
there is no difference between ``VArray`` and ``Array`` interfaces.

Once you have located required array or virtual array in your collection, to access its actual data
you need just to create a slice from your ``Array`` or ``VArray`` object, thus getting ``Subset``
or ``VSubset``.

``Subset`` and ``VSubset`` also have the same interface and they are the final lazy objects, which
expose methods for data update, read and clear.

Creating Collection
-------------------

To create a new ``Сollection`` you will need a few more objects:

   * :class:`DimensionSchema <deker.schemas.DimensionSchema>`
     and/or :class:`TimeDimensionSchema <deker.schemas.TimeDimensionSchema>`
   * :class:`ArraySchema <deker.schemas.ArraySchema>`
     or :class:`VArraySchema <deker.schemas.VArraySchema>`
   * :class:`AttributeSchema <deker.schemas.AttributeSchema>`

And optionally you may need:

- :class:`HDF5Options <deker_local_adapters.storage_adapters.hdf5.hdf5_options.HDF5Options>` and :class:`HDF5CompressionOpts <deker_local_adapters.storage_adapters.hdf5.hdf5_options.HDF5CompressionOpts>`
- :class:`Scale <deker.types.public.classes.Scale>`


Understanding Array and VArray
================================

Array
------
As previously mentioned, ``Array`` is an abstract wrapper over files containing data. It does not have a direct access
to the data, but it knows everything about its properties and options.

.. |cell| image:: images/cell.png
   :scale: 5%

| An array is made of cells - |cell| - containers for data pieces.
| Here is an example of a simple 3-dimensional array with some weather data:

.. image:: images/array_0_axes.png
   :scale: 30%

.. image:: images/legend.png
   :scale: 28%
   :align: right

Let's assume that ``x`` and ``y`` axes represent some geographical grid, and ``z`` axis represents layers
with weather data, as shown in the legend.

It is a single ``Array`` having 4 cells in each dimension, in other words its shape is ``(4, 4, 4)``.

Deker stores this data in a single file, and when we call this ``Array`` from the correspondent ``Collection``, all the
operations with its data will affect this file.

So, it is quite simple: one ``Array`` - one file.


VArray
-------
And here comes ``VArray``!

.. image:: images/array_0_axes.png
   :scale: 30%

No, it is not a mistake. Both ``Array`` and ``VArray`` have the same interface, so there is no visible
difference between them for a user.

.. attention:: But there is a difference under the hood!

Imagine that you need to create a photo of something really big with a very high resolution, for example a photo
of the Earth. Suppose the size of the image is 300.000 px * 200.000 px. It is really huge and requires incredibly
much space on a drive and a lot of RAM to be processed. Obviously, nowadays it is impossible to upload it promptly
to physical memory. Moreover, it may require several storage drives to be written down, as its final size depends
on the data type.

How this problem can be solved? We can make a lot of small shots, place them in separated files and arrange them
in the correct order. We certainly will not be able to see the full picture, but we will be able to browse it piece
by piece.

.. image:: images/vgrid.png
   :scale: 35%
   :align: center

``VArray`` is a virtual wrapper over such a set of files. You can see how ``vgrid`` cuts it into separated pieces
in the above image. Each separate piece is an ``Array``, which lays under ``VArray``. And as previously stated, one
``Array`` is one file. If your ``Collection`` is a collection of ``VArrays``, you don't have to worry about ``Arrays``,
``VArray`` manages them for you.

When we query some piece of data, ``VArray`` calculates which files to open and what bounds to impose on each
of these files.

For example, we have the same VArray: its shape is ``(4, 4, 4)``, its dimensions are arranged as ``['x', 'y', 'z']``
and its *zero-index* is at the front-left-bottom corner.

.. image:: images/varray.png
   :scale: 30%

Let's query it in the following way: ``VArray[:, 2:4, :]``

.. image:: images/varray_request.png
   :scale: 30%

Here you can see, that all of 4 files will be affected, but only the highlighted parts of them will be captured.
If you use these bounds for **inserting or updating**, ``VArray`` will distribute your input data within the proper
files and in the correct order. If you use them for **reading**, ``VArray`` will aggregate all the captured parts into
one ``numpy.ndarray`` of the correspondent shape and in the correct order and return it to you. And, obviously, the
captured parts will be **cleared**, if you so wish.

Pursuing the aim to be fast, ``VArray`` uses its own ``ThreadPoolExecutor`` to cope with all the tasks it needs to do.
In the interest of thread-safety Deker uses its own file locks for all sorts of file operations: creating, reading,
writing and deleting.
