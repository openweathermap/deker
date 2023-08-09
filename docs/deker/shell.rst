*****************
Interactive Shell
*****************

Interactive shell is a convenient Python REPL interface that allows you to manage, query and modify
data in your Deker storage.

.. note:: Deker shell is based on amazing ptpython_ - a better Python REPL

.. _ptpython: https://github.com/prompt-toolkit/ptpython

Features
========

* Autocompletion
* Syntax highlighting
* ``client`` and ``collections`` variables initialized at start
* Shortcut ``use`` function to change current ``collection``
* Imported at start: ``numpy`` as ``np``, ``datetime`` and all ``deker`` public classes
* Running ``asyncio`` loop (thus, enabling you to use ``async`` and ``await``)
* All the ``ptpython`` features


Start
=====

Once installed, open your terminal and run the shell providing path to Deker storage via command
line parameter (in this case it would be ``/tmp/deker-data``)::

    deker file:///tmp/deker-data


Examples
========

Using global collection variable:

.. image:: images/shell_collection.png

Creating a new collection:

.. image:: images/shell_highlight.png

REPL menu (called with ``F2``):

.. image:: images/shell_menu.png
   :scale: 45%


Interface
=========

Imported Deker Classes
----------------------

Basic storage access and management classes:

* :class:`Client <deker.client.Client>`
* :class:`Collection <deker.collection.Collection>`
* :class:`Array <deker.arrays.Array>`
* :class:`VArray <deker.arrays.VArray>`
* :class:`Subset <deker.subset.Subset>`
* :class:`VSubset <deker.subset.VSubset>`

Collection schema related:

* :class:`DimensionSchema <deker.schemas.DimensionSchema>`
* :class:`TimeDimensionSchema <deker.schemas.TimeDimensionSchema>`
* :class:`ArraySchema <deker.schemas.ArraySchema>`
* :class:`VArraySchema <deker.schemas.VArraySchema>`
* :class:`AttributeSchema <deker.schemas.AttributeSchema>`
* :class:`Scale <deker.types.public.classes.Scale>`

Physical storage (HDF5) level options:

* :class:`HDF5Options <deker_local_adapters.storage_adapters.hdf5.hdf5_options.HDF5Options>`
* :class:`HDF5CompressionOpts
  <deker_local_adapters.storage_adapters.hdf5.hdf5_options.HDF5CompressionOpts>`

Preset Variables
----------------

* ``client``: Client (registry of collections) instance, connected to the uri-database
* ``collections``: list of Client collections names
* ``collection``: global default collection variable, set by use("coll_name") method;
* ``np``: numpy library
* ``datetime``: datetime library

Functions
---------

* ``use("collection_name")``: gets collection from client and saves it to ``collection`` variable
* ``get_global_coll_variable()``: returns ``collection`` global variable

