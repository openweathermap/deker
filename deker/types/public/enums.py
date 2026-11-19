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
from enum import Enum

__all__ = ("DEFAULT_INTEGRITY_CHECKS", "IntegrityCheck", "IntegrityFindingKind")


class IntegrityCheck(str, Enum):
    """Named checks of the storage integrity, from the cheapest to the most expensive.

    The first four read metadata and symlinks only; ``data`` reads one cell of every array and
    ``counters`` reads every array whole.
    """

    collections = "collections"  # every collection initialises and has its lock file
    markers = "markers"  # no markers of VArray writes are left by Deker < 2
    arrays = "arrays"  # every main file initialises an Array or a VArray, an Array agrees with its VArray
    links = "links"  # main files and symlinks point at each other, one array per primary attributes
    data = "data"  # one cell of every array reads back with the dtype of the collection
    counters = "counters"  # the counter of empty cells equals the number of fill values in the data


# the checks that read metadata and symlinks only, no data
DEFAULT_INTEGRITY_CHECKS: tuple[IntegrityCheck, ...] = (
    IntegrityCheck.collections,
    IntegrityCheck.markers,
    IntegrityCheck.arrays,
    IntegrityCheck.links,
)


class IntegrityFindingKind(str, Enum):
    """What exactly an integrity check has found."""

    collection_unreadable = "collection_unreadable"
    collection_outdated = "collection_outdated"
    lock_missing = "lock_missing"
    lock_orphaned = "lock_orphaned"
    marker_left = "marker_left"
    meta_unreadable = "meta_unreadable"
    symlink_missing = "symlink_missing"
    symlink_broken = "symlink_broken"
    symlink_misplaced = "symlink_misplaced"
    duplicate_arrays = "duplicate_arrays"
    stray_file = "stray_file"
    varray_missing = "varray_missing"
    position_out_of_grid = "position_out_of_grid"
    time_attribute_wrong = "time_attribute_wrong"
    data_unreadable = "data_unreadable"
    dtype_mismatch = "dtype_mismatch"
    counter_drifted = "counter_drifted"
