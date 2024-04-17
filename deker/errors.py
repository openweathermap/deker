# deker - multidimensional arrays storage engine
# Copyright (C) 2023  OpenWeather
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
from typing import List


class DekerBaseApplicationError(Exception):
    """Base attribute exception."""


class DekerClientError(DekerBaseApplicationError, RuntimeError):
    """If something goes wrong in Client."""

    pass


class DekerArrayError(DekerBaseApplicationError):
    """If something goes wrong in Array."""


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
    """If something goes wrong during validation."""

    pass


class DekerIntegrityError(DekerBaseApplicationError):
    """If something goes wrong during integrity check."""

    pass


class DekerLockError(DekerBaseApplicationError):
    """If a Collection or a Array or VArray instance is locked."""

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
    and passed to this class along with some message.

    ```
    futures = [executor.submit(func, arg) for arg in iterable]

    exceptions = []
    for future in futures:
        try:
            future.result()
        except Exception as e:
            exceptions.append(repr(e) + "\n" + traceback.format_exc(-1))
    ```
    """

    def __init__(self, message: str, exceptions: List[str]):
        self.message = message
        self.exceptions = exceptions
        super().__init__(message)

    def __str__(self) -> str:
        enumerated = [f"{num}) {e}" for num, e in enumerate(self.exceptions, start=1)]
        joined = "\n".join(str(e) for e in enumerated)
        return f"{self.message}; exceptions:\n\n{joined} "


class DekerMemoryError(DekerBaseApplicationError, MemoryError):
    """Early memory overflow exception."""


class DekerVSubsetError(DekerSubsetError, DekerExceptionGroup):
    """If something goes wrong while VSubset managing.

    Regarding that VSubset's inner Subsets' processing
    is made in an Executor, this exception actually is
    an `exceptions messages group`.
    """
