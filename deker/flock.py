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

from pathlib import Path
from typing import Any

from deker.errors import DekerLockError
from deker.log import SelfLoggerMixin
from deker.types.private.enums import LocksExtensions


class Flock(SelfLoggerMixin):
    """File locker.

    :param file: path to the file to be locked
    """

    def __init__(self, file: Path) -> None:
        self.file = file
        self.fd = None  # file descriptor, closed on Flock release
        self.logger.debug("Instantiated")

    def acquire(self) -> None:
        """Create and lock lockfile."""
        # If the file doesn't end with .lock or .arrlock,
        # then it's array or collection lock (e.g .json/.hdf)
        self.logger.debug(f"Trying to acquire lock for {self.file}")
        if str(self.file).endswith(
            (LocksExtensions.array_lock.value, LocksExtensions.collection_lock.value)
        ):
            if not self.file.exists():
                self.file.parent.mkdir(parents=True, exist_ok=True)
            mode = "w"
        else:
            mode = "r"
        try:
            self.fd = open(self.file, mode=mode)  # noqa[PTH123, SIM115]
            self.logger.debug(f"Opened descriptor for {self.file}")
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[arg-type]
            self.logger.debug(f"{self.file} acquired lock")
        except FileNotFoundError:
            self.logger.debug(f"{self.file} not found")
        except BlockingIOError:
            if self.fd:
                self.fd.close()
                self.logger.debug(f"Closed descriptor for {self.file} due to BlockingIOError")
            raise DekerLockError(f"{self.file} is locked")

    def release(self) -> None:
        """Releases lockfile."""
        self.logger.debug(f"trying to release lock for {self.file}")
        if self.fd:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()
            self.logger.debug(f"{self.file} released lock")
        if self.file.name.endswith(LocksExtensions.array_lock.value):
            self.file.unlink(missing_ok=True)
            self.logger.debug(f"{self.file} unlinked symlink")

    def __enter__(self) -> "Flock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        return self.release()
