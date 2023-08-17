********************
Connecting to Server
********************

To access data remotely stored on [OpenWeather](https://openweathermap.org) managed Deker server infrastructure, you need to use server adapters

It is an original OpenWeather plugin, based on `httpx <https://www.python-httpx.org/>`_
with HTTP 2.0 support that allows your local client to communicate with remote server instances of Deker.

Deker will automatically find and initialize this plugin if it is installed in current environment.

.. attention::
   You must install ``deker-server-adapters`` package , for details refer to the `Installation page`_


Usage
=========
To use server version, you have to initialize Deker's Client with an uri  which contains ``http/https`` schema.

.. code-block:: python

    from deker import Client
    client = Client("http://{url-to-deker-server}") # As simple as that

And now the client will communicate with Deker server

If authentication is enabled on the Deker server, you can provide credentials by adding it yo the url like this:

.. code-block:: python
  
   from deker import client
   client = Client("https://{username}:{password}@{url-to-deker-server}")

Configuration
=============
Server adapters use ``httpx client`` under the hood. You can configure its behaviour by passing
keyword arguments to the ``httpx_conf`` argument of the Deker's Client

.. code-block:: python

    import httpx
    from deker import Client

    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    client = Client("http://{url-to-deker-server}", httpx_conf={'http2': False, 'timeout': 10, 'limits': limits})

Full list of available config you can find at `httpx website <https://www.python-httpx.org/api/#client>`_

By default, adapters' ``httpx client`` has following settings:

.. list-table::
   :header-rows: 1

   * - Key
     - Type
     - Default Value
   * - verify
     - bool
     - True
   * - http2
     - bool
     - True
   * - timeout
     - Optional[float]
     - None

Errors
=========

.. py:exception:: DekerServerError(response: Response, message: str)
   
   Bases: :class:`DekerBaseApplicationError`
   
   Server error, which is raised on any non-specific ocasion (like 5xx status from server)
   
   :param response: Httpx Response
   :param message: Message of exception

.. py:exception:: DekerTimeoutServer

   Bases: :class:`DekerServerError`

   This exception is raised on any timeout (Httpx's Timeout exception or 504 status )

.. py:exception:: DekerBaseRateLimitError(message: str, limit: Optional[int], remaining: Optional[int], reset: Optional[int])

   Bases: :class:`DekerBaseApplicationError`



   :param message: Exception message
   :param limit: What is the limit for the user (requests per second)
   :param remaining: How much is left (requests per second)
   :param reset: When limits will be reset

.. py:exception:: DekerRateLimitError

   Bases: :class:`DekerBaseRateLimitError`


   If user's limit is exceeded

.. py:exception:: DekerDataPointsLimitError

   Bases: :class:`DekerBaseRateLimitError`

   If requested subset exceeds quota

.. _Installation page: installation.html

