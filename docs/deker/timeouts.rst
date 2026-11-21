*************************
Timeouts and Cancellation
*************************

DEKER™ limits how long an operation may take, and an application may stop the operations it has
started. A read and a write are limited differently: a read is stopped as a whole, a write is never
interrupted once it has started changing files.


Reading
=======

A read is limited by its timeout from the start to the end. By default it is ``read_timeout`` of
the client, ``60`` seconds; a single read may be given its own ``timeout``::

   from deker import Client

   with Client(uri, read_timeout=120) as client:
      collection = client.get_collection("weather")
      array = collection.filter({"id": array_id}).last()

      data = array[:].read()  # up to 120 seconds
      data = array[:].read(timeout=10)  # up to 10 seconds

When the time is over, the read raises ``DekerTimeoutError`` and returns nothing. The timeout is
checked before the data is read, after the storage has returned it and, for a ``VArray``, before
each of its ``Arrays``. A file that is being read when the time is over is read to the end, and its
data is thrown away.


Writing
=======

A write has no time limit of its own: once it has started changing a file it goes to the end. What
is limited is the wait for the lock of each file the write needs, ``write_lock_timeout`` of the
client, ``60`` seconds by default. A single write may be given its own ``timeout``::

   with Client(uri, write_lock_timeout=30) as client:
      collection = client.get_collection("weather")
      array = collection.filter({"id": array_id}).last()

      array[:].update(data)  # waits for the lock up to 30 seconds
      array[:].clear(timeout=5)  # waits for the lock up to 5 seconds

When the lock is not released in time, the write raises ``DekerLockError`` and changes nothing in
that file. A ``VArray`` is written ``Array`` by ``Array``, each under its own lock: the ``Arrays``
that got their locks are written, and the error tells how many were not.

Creating and clearing a collection wait for the lock of the collection the same way, instead of
failing at once when another process holds it.


Methods
=======

The ``timeout`` argument is keyword-only. Reads:

* ``Subset.read`` and ``VSubset.read``

Writes, where ``timeout`` limits the wait for each lock:

* ``Subset.update``, ``Subset.clear``, ``VSubset.update`` and ``VSubset.clear``
* ``Array.delete``, ``VArray.delete``, ``Array.update_custom_attributes`` and
  ``VArray.update_custom_attributes``
* ``Collection.create``, ``Collection.clear`` and ``Collection.delete``
* ``Client.create_collection`` and ``Client.collection_from_dict``

A timeout is a positive integer number of seconds; anything else, ``bool`` and ``None`` included,
raises ``DekerValidationError`` before the operation starts. The same holds for ``read_timeout``
and ``write_lock_timeout`` of the client.


Cancellation
============

An application that runs DEKER™ on behalf of someone else, a server for example, may need to stop
the operations of a request when that request is gone. For this ``deker_tools.cancel`` provides
``cancellation_scope``: the operations started inside it are cancelled together with the token
given to it. The token is any token of the `cantok`_ library::

   from cantok import SimpleToken
   from deker_tools.cancel import cancellation_scope

   token = SimpleToken()

   with cancellation_scope(token):
      data = array[:].read()

   # elsewhere, in another thread:
   token.cancel()

A cancelled read raises ``DekerCancelledError``; ``DekerTimeoutError`` is its subclass, so one
``except DekerCancelledError`` catches both. A cancelled write stops only if none of its files has
been started yet, and raises ``DekerCancelledError`` then; once a file has been started, the write
goes to its end whatever happens to the token, so a cancellation never leaves a half-written file
or a ``VArray`` written partly.

The scope is read when an operation starts, in the thread that starts it; the threads of the
operation get its token from it, not from the scope. Outside any scope a write cannot be cancelled,
and a read is limited only by its timeout.

.. _cantok: https://cantok.readthedocs.io
