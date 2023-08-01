.. currentmodule:: deker

*************
Installation
*************

Deker installation
====================

Deker was developed and tested on Linux (``Ubuntu 20.04``, ``Centos 8.7``) and MacOS (``12.6.3``, ``13.14.1`` ),
so these platforms are perfectly suitable for using Deker.

.. note:: Minimal python version for Deker is ``3.9``.

.. attention:: If you are a user of M1+ chip, please, refer to the `ARM architecture family`_ section first.

Required dependencies
---------------------
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

Deker comes with the previously mentioned dependencies included::

    pip install deker

or::

    python -m pip install deker

Extra dependencies
------------------
- xarray>=2023.5.0

.. _Xarray: https://docs.xarray.dev/en/stable/getting-started-guide/installing.html
.. _pandas: https://pandas.pydata.org/getting_started.html

If you wish to convert your data into Xarray_ or pandas_ *(or even some other)* objects::

    pip install deker[xarray]

or ::

    python -m pip install deker[xarray]

Or you can install them separately::

    pip install deker
    pip install xarray

or ::

    python -m pip install deker
    python -m pip install xarray

ARM architecture family
----------------------------
| Deker uses NumPy, and some NumPy types are unsupported on current NumPy ARM version.
| If you want to run Deker library on your Mac with M1+ chip inside, you need to install ``python x86_64`` with Rosetta_.

.. _Rosetta: https://support.apple.com/en-us/HT211861

Use this guide_ or follow next steps:

.. _guide: https://towardsdatascience.com/how-to-use-manage-multiple-python-versions-on-an-apple-silicon-m1-mac-d69ee6ed0250

1. Install Rosetta (ARM -> x86_64 translator)::

    softwareupdate --install-rosetta

2. Create a Rosetta terminal:

   | 2.1. duplicate your terminal ``apps -> utilities -> right click -> duplicate`` or ``install new``
   | 2.2. click ``Get info`` on new terminal and set ``Open using Rosetta``

3. Install homebrew::

    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

4. Add an alias to your ``zsh`` config file::

    alias rbrew="arch -x86_64 /usr/local/bin/brew"

5. Install python::

    rbrew install python@3.10

**Hooray! Now you can install Deker with pip!**

Interactive shell
===================
``Deker-shell`` is a MVP built on ``ptpython`` which provides a minimalistic interactive shell interface,
where you can manage your Deker database in real time. Requires ``deker`` package to be installed alongside manually.

It comes with **code autocompletion**, **syntax highlighting** and session **actions history**.

Installation
--------------
Deker-shell is not included as an out-of-box battery for Deker, so it should be installed manually::

   pip install deker deker-shell

or ::

   python -m pip install deker deker-shell

Usage
--------------
Once installed, open your terminal and make ::

   deker file://<path-to-your-deker-storage>

You will be brought to the running Python REPL with:
   - imported NumPy as ``np``, ``datetime`` library and Deker public classes
   - predefined variables ``client`` and ``collections``
   - a running asyncio loop; thus, you can use ``async/await`` right in it

Deker tools
================
``Deker-tools`` is an out-of-box battery and provides several tools and utilities. You may find this package useful
in projects, even those not related to Deker.

Installation
--------------
::

   pip install deker-tools

or ::

   python -m pip install deker-tools


Usage
--------------
You will get a collection of utility functions and classes designed to assist in common data processing tasks.
It consists of modules that handle data conversion, path validation, and slice manipulation.

data
+++++++++

This module provides ``convert_size_to_human`` method for converting bytes size into human readable representation::

    >>> convert_size_to_human(1052810)
    "1.0 MB"

path
+++++++++
This module provides functions to validate and handle filesystem ``paths``::

    is_empty(path)
    is_path_valid(path)

slices
+++++++++
Calculate ``shape`` of a subset from the index expression::

    >>> shape = (361, 720, 4)
    >>> index_exp = (slice(None, None, None), slice(None, None, None), 0)
    >>> create_shape_from_slice(shape, index_exp)
    (361, 720)

Convert ``slice`` into a sequence and get its length::

    >>> match_slice_size(10, slice(10))
    (0, 10, 1)

Serialize ``slices`` to ``string`` and vice versa with ``slice_converter``::

    >>> slice_converter[5]
    '[5]'

    >>> slice_converter[datetime.datetime(2023,1,1):datetime.datetime(2023,2,1), 0.1:0.9:0.05]
    '[`2023-01-01T00:00:00`:`2023-02-01T00:00:00`, 0.1:0.9:0.05]'

time
+++++++++

This module provides ``get_utc`` function which returns timezone with UTC or current time by default::

    >>> get_utc()
    2023-07-26 15:42:05.539317+00:00

    >>> get_utc(datetime.now())
    2023-07-26 15:42:05.539317+00:00

The contents of this package may be changed anytime. For details refer to the `deker-tools API`_

.. _deker-tools API: api/deker_tools/modules.html


.. note:: Please, don't hesitate to inform us about any installation or usage issues.
