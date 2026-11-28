*****************
Interactive Shell
*****************

Interactive shell is a convenient Python REPL interface that allows you to manage, query and modify
data in your DEKER™ storage.

.. note:: DEKER™ shell is based on amazing ptpython_ - a better Python REPL

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

Once installed, open your terminal and run the shell with the URI of your DEKER™ storage as the
command line argument::

    deker file:///tmp/deker-data

URI schemes
-----------

The scheme of the URI tells the shell where the storage is and which adapters serve it. The
package of the adapters must be installed; when it is missing, the shell stops before start with
the ``pip install`` command to run.

.. list-table::
   :header-rows: 1
   :widths: 20 45 35

   * - Scheme
     - Storage
     - Adapters
   * - ``file://``
     - a local directory: ``file:///tmp/deker-data``
     - ``deker-local-adapters``
   * - ``http://``
     - a remote DEKER™ server: ``http://host:port``
     - ``deker-server-adapters``
   * - ``https://``
     - the same over TLS, with credentials if the server asks for them:
       ``https://user:password@host``
     - ``deker-server-adapters``

See `Connecting to Server <connecting_to_server.html>`_ for the server URIs and the settings of
the server adapters.

Command line options
--------------------

The options of the DEKER™ ``Client`` are options of the ``deker`` command; the ones not given
keep the defaults of the client.

.. list-table::
   :header-rows: 1
   :widths: 38 62

   * - Option
     - Meaning
   * - ``-w``, ``--workers INTEGER``
     - number of threads for DEKER™
   * - ``-t``, ``--write-lock-timeout INTEGER``
     - seconds a writer waits for a locked file to be released
   * - ``-c``, ``--write-lock-check-interval INTEGER``
     - seconds a writer sleeps between the checks of a lock
   * - ``-l``, ``--loglevel TEXT``
     - level of the DEKER™ loggers: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``
   * - ``-m``, ``--memory-limit TEXT``
     - memory allowed for one array or subset: bytes, or ``100K``, ``512M``, ``4G``; ``0`` or
       less means the total RAM plus swap
   * - ``--history-file FILE``
     - file keeping the command history between sessions, ``~/.deker_shell_history`` by default
   * - ``--history-size INTEGER``
     - commands kept in the history file, ``1000`` by default; the oldest ones are dropped
   * - ``--help``
     - the list of options

Any other option is passed to the ``Client`` as a keyword argument, its value parsed as a
Python literal when it is one (``10``, ``1.5``, ``True``, ``None``; anything else stays a string).
A dotted option fills a dictionary: ``--httpx_conf.timeout 10 --httpx_conf.http2 True`` becomes
``httpx_conf={"timeout": 10, "http2": True}``, the way the server adapters take their settings::

    deker https://user:password@host --workers 8 --loglevel INFO --httpx_conf.timeout 30

History and completion
----------------------

The command history is kept between the sessions in the history file. ``F3`` opens it, the arrow
keys search it from what is typed, and the completion is fuzzy: a few letters of a name in any
order are enough. ``use(`` completes the names of the collections of the storage; the private
names (starting with ``_``) are hidden until ``_`` is typed.

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

Imported DEKER™ Classes
------------------------

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

Miscellaneous
--------------
A path to a ``.py`` file instead of a URI runs that script in the environment of the shell, without
starting the REPL::

   deker my_python_script.py
