.. toctree::
   :hidden:
   :caption: Tutorials

   Installation <deker/installation>
   Collection Schema <deker/collection_schema>
   Data Access <deker/data_access>
   Fine Tuning <deker/fine_tuning>
   Interactive Shell <deker/shell>
   Deker Tools <deker/tools>
   Connecting to Server <deker/connecting_to_server>


.. toctree::
   :hidden:
   :caption: API Reference

   Deker API <deker/api/modules>
   Deker Tools <deker/api/deker_tools/modules>

.. toctree::
   :hidden:
   :caption: About Us

   OpenWeather <https://openweathermap.org>
   GitHub Projects <https://github.com/openweathermap>


**************
What is Deker?
**************

.. image:: deker/images/logo.png
   :align: right
   :scale: 50%

Deker is pure Python implementation of petabyte-scale highly parallel data storage engine for
multidimensional arrays.

Deker name comes from term *dekeract*, the 10-cube_.

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

.. _10-cube: https://en.wikipedia.org/wiki/10-cube


Features
========

* **Open source** under GPL 3.0
* Scalable storage of huge virtual arrays via **tiling**
* **Parallel processing** of virtual array tiles
* Own **locking** mechanism enabling arrays parallel read and write
* Array level **metadata attributes**
* **Fancy data slicing** using timestamps and named labels
* Support for industry standard NumPy_, pandas_ and Xarray_
* Storage level data **compression and chunking** (via HDF5)

.. _NumPy: https://numpy.org/doc/stable/
.. _pandas: https://pandas.pydata.org/docs/
.. _Xarray: https://docs.xarray.dev/en/stable/


Code and Documentation
======================

Open source implementation of Deker storage engine is published at

  * https://github.com/openweathermap/deker

API documentation and tutorials for the current release could be found at

  * https://docs.deker.io
