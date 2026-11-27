# deker - multidimensional arrays storage engine
# Copyright (C) 2023-2026  Openweather Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from types import TracebackType

from deker_tools.cancel import TokenCancelledError, TokenTimeoutError


class DekerBaseApplicationError(Exception):
    """Base exception of Deker: every Deker error is its subclass."""


class DekerClientError(DekerBaseApplicationError, RuntimeError):
    """If something goes wrong in Client."""

    pass


class DekerAdapterClosedError(DekerClientError):
    """If an adapter or a factory is used after the client context was closed.

    It is a ``DekerClientError``: every object of a closed client answers with it,
    while the client itself raises a plain ``DekerClientError``.
    """


class DekerArrayError(DekerBaseApplicationError):
    """If something goes wrong in Array or VArray: a missing file, a creation of an array that exists."""


class DekerArrayTypeError(DekerArrayError):
    """If final Array's or VArray's values type do not match the dtype, set in Collection schema."""

    pass


class DekerCollectionError(DekerBaseApplicationError):
    """If something goes wrong in Collection."""

    pass


class DekerCollectionAlreadyExistsError(DekerCollectionError):
    """If collection with the same name already exists."""

    pass


class DekerCollectionNotExistsError(DekerCollectionError):
    """If collection doesn't exist."""

    pass


class DekerInvalidSchemaError(DekerBaseApplicationError):
    """If schema is invalid."""

    pass


class DekerMetaDataError(DekerInvalidSchemaError):
    """If metadata is invalid/corrupted."""

    pass


class DekerFilterError(DekerBaseApplicationError):
    """If something goes wrong during filtering."""

    pass


class DekerValidationError(DekerBaseApplicationError):
    """If something goes wrong during validation.

    :param args: the message, as of any exception
    :param parameters: names of the dictionaries of the arguments the error is in - ``primary_attributes``, then
      ``custom_attributes``; empty if the error is not in them
    """

    def __init__(self, *args: object, parameters: tuple[str, ...] = ()) -> None:
        super().__init__(*args)
        self.parameters = parameters


class DekerIntegrityError(DekerBaseApplicationError):
    """If the integrity check is asked to stop at the first finding and finds one."""

    pass


class DekerLockError(DekerBaseApplicationError):
    """If a lock cannot be taken: a reader meets a writer, or a writer does not get the file within the timeout."""

    pass


class DekerInstanceNotExistsError(DekerBaseApplicationError):
    """If instance was deleted, but user is trying to call methods on it."""

    pass


class DekerInvalidManagerCallError(DekerBaseApplicationError):
    """If we try to call method that shouldn't be called.

    E.g., .varrays in Arrays Collection
    """


class DekerSubsetError(DekerArrayError):
    """If something goes wrong while Subset managing."""


class DekerExceptionGroup(DekerBaseApplicationError):
    """If one or more threads finished with any exception.

    Name, message and reasonable tracebacks of all
    of these exceptions shall be collected in a list
    and passed to this class along with some message::

        futures = [executor.submit(func, arg) for arg in iterable]

        exceptions = []
        for future in futures:
            try:
                future.result()
            except Exception as e:
                exceptions.append(repr(e) + "\n" + traceback.format_exc(-1))
    """

    def __init__(self, message: str, exceptions: list[str]):
        self.message = message
        self.exceptions = exceptions
        super().__init__(message, exceptions)

    def __str__(self) -> str:
        enumerated = [f"{num}) {e}" for num, e in enumerate(self.exceptions, start=1)]
        joined = "\n".join(str(e) for e in enumerated)
        return f"{self.message}; exceptions:\n\n{joined} "


class DekerMemoryError(DekerBaseApplicationError, MemoryError):
    """Early memory overflow exception."""


class DekerVSubsetError(DekerSubsetError, DekerExceptionGroup):
    """If something goes wrong while VSubset managing.

    A ``VSubset`` reads, writes and clears the ``Arrays`` of its ``VArray`` one by one in an executor,
    so this exception is a group of the messages of the ``Arrays`` that failed; the other ``Arrays``
    are processed.
    """


class DekerCancelledError(DekerBaseApplicationError):
    """The operation is cancelled from outside, for instance its client went away; nothing is changed by it.

    A write is cancelled only before any of its files is started; once one is started, the write goes to its end.
    """


class DekerTimeoutError(DekerCancelledError):
    """The read took longer than its timeout; no data are returned."""


class TokenErrorConverter:
    """Turn the errors of the cancellation tokens of deker-tools into the errors of DEKER.

    Every public operation of DEKER runs its work inside: an operation stopped at one of its cancellation points
    raises ``DekerTimeoutError`` or ``DekerCancelledError`` with the text of the token error and without its
    traceback. Other exceptions pass as they are.
    """

    def __enter__(self) -> "TokenErrorConverter":
        """Enter the work of the operation.

        :returns: the converter itself
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Raise the error of DEKER for a token error of the work.

        :param exc_type: type of the exception raised in the work, if any
        :param exc_value: the exception raised in the work, if any
        :param traceback: traceback of the exception, if any
        :raises DekerTimeoutError: if a timeout token stopped the work
        :raises DekerCancelledError: if any other token stopped the work
        """
        if isinstance(exc_value, TokenTimeoutError):
            raise DekerTimeoutError(str(exc_value)) from None
        if isinstance(exc_value, TokenCancelledError):
            raise DekerCancelledError(str(exc_value)) from None
