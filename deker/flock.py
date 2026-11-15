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

import fcntl
import os

from pathlib import Path
from typing import Any

from deker.errors import DekerLockError
from deker.log import SelfLoggerMixin
from deker.types.private.enums import LocksExtensions


class Flock(SelfLoggerMixin):
    """File locker.

    The lock is an advisory ``flock`` on a descriptor of the file; a directory is locked the same way.
    It lives in the kernel: it is released with the descriptor and dies with its holder.

    :param file: path to the file or directory to be locked
    :param shared: take a shared lock (several holders at once, no exclusive holder) instead of an exclusive one
    """

    def __init__(self, file: Path, shared: bool = False) -> None:
        self.file = file
        self.shared = shared
        self.fd: int | None = None  # file descriptor, closed on Flock release
        self.logger.debug("Instantiated")

    def acquire(self) -> None:
        """Open the file (or directory) and lock it.

        :raises DekerLockError: if the file is already locked by another holder
        """
        # a collection lock file is created on the way; any other path (a data file, a directory) is locked as it is
        self.logger.debug(f"Trying to acquire lock for {self.file}")
        if str(self.file).endswith(LocksExtensions.collection_lock.value):
            if not self.file.exists():
                self.file.parent.mkdir(parents=True, exist_ok=True)
            flags = os.O_WRONLY | os.O_CREAT
        else:
            flags = os.O_RDONLY
        fd = os.open(self.file, flags)
        self.logger.debug(f"Opened descriptor for {self.file}")
        try:
            fcntl.flock(fd, (fcntl.LOCK_SH if self.shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            self.logger.debug(f"Closed descriptor for {self.file} due to BlockingIOError")
            raise DekerLockError(f"{self.file} is locked")
        self.fd = fd
        self.logger.debug(f"{self.file} acquired {'shared' if self.shared else 'exclusive'} lock")

    def release(self) -> None:
        """Unlock the file and close its descriptor."""
        self.logger.debug(f"trying to release lock for {self.file}")
        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None
            self.logger.debug(f"{self.file} released lock")

    def __enter__(self) -> "Flock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return self.release()
