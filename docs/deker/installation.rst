************
Installation
************


Deker
=====

Deker was developed and tested on x86_64 Linux and both x86_64 and Apple silicon MacOS, and known
to be running in production environments on x86_64 Linux servers.

.. note:: Minimal Python version for Deker is ``3.9``.

.. attention:: If you run MacOS on Apple silicon (M series CPU), please, refer to the `Running on
               Apple Silicon`_ section first.


Dependencies
------------

Deker depends on the following third-party packages:

    * ``numpy`` >= 1.18
    * ``attrs`` >= 23.1.0
    * ``tqdm`` >= 4.64.1
    * ``psutil`` >= 5.9.5
    * ``h5py`` >= 3.8.0
    * ``hdf5plugin`` >= 4.0.1

Also please not that for future flexibility few internal Deker components are published as separate
packages:

    * ``deker-tools``
    * ``deker-local-adapters``

To install Deker with all the previously mentioned dependencies, run::

    pip install deker


Optional Packages
-----------------

Deker also supports output of its data as pandas_ or Xarray_ via the following package:

    * ``xarray`` >= 2023.5.0

To install it with ``xarray`` optional dependency::

    pip install deker[xarray]

.. _Xarray: https://docs.xarray.dev/en/stable/getting-started-guide/installing.html
.. _pandas: https://pandas.pydata.org/getting_started.html


Running on Apple Silicon
------------------------

Deker uses NumPy, and some NumPy types are unsupported on current NumPy arm64 version. So if you
want to use Deker library on Apple silicon (M series CPU), you have to install x86_64 version of
Python using Rosetta_ x86_64 to arm64 dynamic binary translator.

You may use the following guide_ to install x86_64 version of Python an then switch to that version
in your Deker project using ``pyenv`` and install Deker package as usual.

.. _Rosetta: https://developer.apple.com/documentation/apple-silicon/about-the-rosetta-translation-environment
.. _guide: https://sixty-north.com/blog/pyenv-apple-silicon.html


Interactive Shell
=================

``deker-shell`` is an interactive environment that enables you to manage and access Deker storage
in a convenient way. It requires ``deker`` package to be installed manually before use as described
above.

Interactive shell comes with **code autocompletion**, **syntax highlighting** and session **actions
history**.

To install interactive shell package::

   pip install deker deker-shell


Deker Tools
===========

``deker-tools`` is an out-of-box battery which provides several useful tools and utilities to work
with Deker data. You may find this package useful in projects, even if they are not related to
Deker.

To install Deker tools package::

   pip install deker-tools

