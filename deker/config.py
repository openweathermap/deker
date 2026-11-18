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

from attr import asdict, dataclass, fields


@dataclass(kw_only=True)
class DekerConfig:
    """Deker application configuration, built by the ``Client`` from its arguments.

    :param uri: Deker uri string
    :param workers: number of threads for VArray management
    :param write_lock_timeout: seconds a writer waits for a locked file to be released
    :param write_lock_check_interval: seconds a writer sleeps between the checks of a lock
    :param memory_limit: RAM size in bytes available for Deker
    :param loglevel: Deker logging level
    :param collections_directory: directory of the collections in the storage
    :param array_data_directory: directory of the files of the ``Arrays`` in a collection
    :param varray_data_directory: directory of the files of the ``VArrays`` in a collection
    :param array_symlinks_directory: directory of the symlinks of the ``Arrays`` in a collection
    :param varray_symlinks_directory: directory of the symlinks of the ``VArrays`` in a collection
    :param skip_collection_create_memory_check: skip the memory check when a collection is created
    :raises ValueError: if a setting has a wrong type
    """

    # configurable attributes
    uri: str
    workers: int
    write_lock_timeout: int
    write_lock_check_interval: int
    memory_limit: int
    loglevel: str = "DEBUG"

    # non configurable default attributes
    collections_directory: str = "collections"
    array_data_directory: str = "array_data"
    varray_data_directory: str = "varray_data"
    array_symlinks_directory: str = "array_symlinks"
    varray_symlinks_directory: str = "varray_symlinks"
    skip_collection_create_memory_check: bool = False

    @property
    def as_dict(self) -> dict:
        """Serialize as dict."""
        return asdict(self)

    def __attrs_post_init__(self) -> None:
        """Check that every setting has the type of its field.

        :raises ValueError: if a setting has a wrong type
        """
        for field in fields(self.__class__):
            default_value = getattr(self, field.name)
            if not isinstance(default_value, field.type):
                raise ValueError(f"'{field.name}' setting has wrong type")  # noqa: TRY004
