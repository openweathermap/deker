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

import fcntl
import os

from pathlib import Path
from typing import Any

from deker.errors import DekerLockError
from deker.log import SelfLoggerMixin


class Flock(SelfLoggerMixin):
    """File locker.

    The lock is an advisory ``flock`` on a descriptor of the file; a directory is locked the same way.
    It lives in the kernel: it is released with the descriptor and dies with its holder.

    :param file: path to the file or directory to be locked
    :param shared: take a shared lock (several holders at once, no exclusive holder) instead of an exclusive one
    :param create: create the file, empty, when it does not exist; its directory never
    """

    def __init__(self, file: Path, shared: bool = False, create: bool = False) -> None:
        self.file = file
        self.shared = shared
        self.create = create
        self.fd: int | None = None  # file descriptor, closed on Flock release
        self.logger.debug("Instantiated")

    def acquire(self) -> None:
        """Open the file (or directory) and lock it.

        :raises DekerLockError: if the file is already locked by another holder: exclusively, or at all when
          an exclusive lock is asked
        :raises FileNotFoundError: if the file does not exist and is not to be created, or its directory does not exist
        """
        self.logger.debug("Trying to acquire lock for %s", self.file)
        # a created file gets the mode a new file of ``open`` gets
        fd = os.open(self.file, os.O_WRONLY | os.O_CREAT if self.create else os.O_RDONLY, 0o666)
        self.logger.debug("Opened descriptor for %s", self.file)
        try:
            fcntl.flock(fd, (fcntl.LOCK_SH if self.shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            self.logger.debug("Closed descriptor for %s due to BlockingIOError", self.file)
            raise DekerLockError(f"{self.file} is locked")
        self.fd = fd
        self.logger.debug("%s acquired %s lock", self.file, "shared" if self.shared else "exclusive")

    @property
    def descriptor(self) -> int:
        """Get the descriptor of the file the lock holds.

        :returns: the descriptor
        :raises DekerLockError: if the lock holds nothing: never acquired, refused or released
        """
        if self.fd is None:
            raise DekerLockError(f"{self.file} is not locked")
        return self.fd

    def release(self) -> None:
        """Unlock the file and close its descriptor."""
        self.logger.debug("trying to release lock for %s", self.file)
        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None
            self.logger.debug("%s released lock", self.file)

    def __enter__(self) -> "Flock":
        """Acquire the lock.

        :returns: the held lock
        """
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Release the lock.

        :param exc_type: type of the exception raised in the block, if any
        :param exc_val: the exception
        :param exc_tb: its traceback
        """
        return self.release()
