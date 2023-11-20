************
Installation
************


Deker
=====

Deker was developed and tested on x86_64 Linux and both x86_64 and Apple silicon macOS, and known
to be running in production environments on x86_64 Linux servers.

.. note::
   Minimal Python version for Deker is ``3.9``.

.. attention::
   Deker uses NumPy, and some NumPy types are unsupported on current NumPy arm64 version. So if you
   want to use Deker library on Apple silicon (M series CPU), you have to install x86_64 version of
   Python using Rosetta_ x86_64 to arm64 dynamic binary translator.

   You may use the following guide_ to install x86_64 version of Python an then switch to that
   version in your Deker project using ``pyenv`` and install Deker package as usual.

.. _Rosetta: https://developer.apple.com/documentation/apple-silicon/about-the-rosetta-translation-environment
.. _guide: https://sixty-north.com/blog/pyenv-apple-silicon.html


Dependencies
------------

Deker depends on the following third-party packages:

    * ``numpy`` >= 1.18
    * ``attrs`` >= 23.1.0
    * ``tqdm`` >= 4.64.1
    * ``psutil`` >= 5.9.5
    * ``h5py`` >= 3.8.0
    * ``hdf5plugin`` >= 4.0.1

Also please note that for flexibility few internal Deker components are published as separate
packages:

    * ``deker-local-adapters``
    * ``deker-server-adapters``
    * ``deker-tools``

To install Deker run:

.. code-block:: bash

    pip install deker


Optional Packages
-----------------

Deker also supports output of its data as Xarray_ via the following package:

    * ``xarray`` >= 2023.5.0

To install it with ``xarray`` optional dependency:

.. code-block:: bash

    pip install deker[xarray]

.. _Xarray: https://docs.xarray.dev/en/stable/getting-started-guide/installing.html

Deker Tools
===========

``deker-tools`` is an out-of-box battery which provides several useful tools and utilities to work
with Deker data. You may find this package useful in your projects, even if they are not related to
Deker.

To install Deker tools package, run:

.. code-block:: bash

    pip install deker-tools


Interactive Shell
=================

``deker-shell`` is an interactive environment that enables you to manage and access Deker storage
in a convenient way. It requires ``deker`` package to be installed **manually** before use as
described above.

To install interactive shell package, run:

.. code-block:: bash

    pip install deker deker-shell

Or you can install it alongside with Deker by:

.. code-block:: bash

    pip install deker[shell]


Server Adapters
===============
.. _plugin: connecting_to_server.html

It is an original OpenWeather plugin_, based on `httpx <https://www.python-httpx.org/>`_
with HTTP 2.0 support, that allows your local client to communicate with remote OpenWeather
public server instances of Deker.

If you don't have Deker yet, run

.. code-block:: bash

    pip install deker[server-adapters]

If you have already installed Deker, you can simply install plugin to use it by:

.. code-block:: bash

    pip install deker-server-adapters


Install All at Once
=================================
You can install all the above mentioned options at once by:

.. code-block:: bash

    pip install deker[all]
