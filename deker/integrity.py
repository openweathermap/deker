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
"""Integrity check of a local storage: named checks, a report of the findings and their repairs.

The checker walks the storage once and builds what lies on the disk (collections, main files, symlinks);
every check is a strategy that looks at one of these and returns findings, and knows how to repair the
findings it marks as repairable. The checks are registered by their names in ``INTEGRITY_CHECKS``.
"""

import json
import os

from abc import ABC, abstractmethod
from collections.abc import Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Generic, NoReturn, TypeVar

import numpy as np
import tqdm

from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.errors import DekerBaseApplicationError, DekerCollectionNotExistsError, DekerIntegrityError
from deker.flock import Flock
from deker.log import SelfLoggerMixin
from deker.tools import array_time_attributes, get_main_path, get_symlink_path
from deker.types import (
    DEFAULT_INTEGRITY_CHECKS,
    IntegrityCheck,
    IntegrityFinding,
    IntegrityFindingKind,
    IntegrityReport,
    LocksExtensions,
)

if TYPE_CHECKING:
    from deker.client import Client


@dataclass
class StorageFiles:
    """The root of the storage as found on the disk.

    :param root: path to the directory of the collections
    :param names: names of the collection directories looked at
    :param lock_names: names of the collections that have a lock file
    :param collections: the collections that initialised
    :param unreadable: the collections that did not, with the reason
    :param outdated: the collections whose file is of an older metadata version, with that version and the
        file as it lies on the disk
    """

    root: Path
    names: list[str] = field(default_factory=list)
    lock_names: list[str] = field(default_factory=list)
    collections: list[Collection] = field(default_factory=list)
    unreadable: dict[str, str] = field(default_factory=dict)
    outdated: dict[str, tuple[str, dict]] = field(default_factory=dict)


@dataclass
class ArrayOnDisk:
    """A main file of a collection and the Array or VArray read from it.

    :param files: the files of the collection
    :param main_file: path to the main file
    :param is_varray: whether the file is a VArray one
    :param array: the array read from the file, ``None`` if it cannot be read
    :param error: why the array cannot be read
    """

    files: "CollectionFiles"
    main_file: Path
    is_varray: bool
    array: Array | VArray | None = None
    error: str = ""

    @property
    def array_id(self) -> str:
        """Id of the array: the main file is named after it."""
        return self.main_file.stem

    @property
    def kind(self) -> str:
        """``Array`` or ``VArray``, for the messages."""
        return "VArray" if self.is_varray else "Array"


@dataclass
class CollectionFiles:
    """What lies on the disk of a collection.

    :param collection: the collection
    :param arrays: the main files of its Arrays
    :param varrays: the main files of its VArrays
    :param array_symlinks: every entry under the Array symlinks directory that is not a directory
    :param varray_symlinks: every entry under the VArray symlinks directory that is not a directory
    """

    collection: Collection
    arrays: list[ArrayOnDisk] = field(default_factory=list)
    varrays: list[ArrayOnDisk] = field(default_factory=list)
    array_symlinks: list[Path] = field(default_factory=list)
    varray_symlinks: list[Path] = field(default_factory=list)

    @property
    def path(self) -> Path:
        """Directory of the collection on the local disk."""
        path = self.collection.path
        if not isinstance(path, Path):  # pragma: no cover  # guard: the checker works on local storages only
            raise DekerIntegrityError(f"Collection {self.collection.name} is not on a local disk: {path}")
        return path

    @property
    def varrays_by_id(self) -> dict[str, VArray]:
        """The VArrays that initialised, by their ids."""
        return {entry.array.id: entry.array for entry in self.varrays if isinstance(entry.array, VArray)}

    @property
    def unreadable_varray_ids(self) -> set[str]:
        """Ids of the VArrays whose main files did not initialise."""
        return {entry.array_id for entry in self.varrays if entry.array is None}


TargetT = TypeVar("TargetT", StorageFiles, CollectionFiles, ArrayOnDisk)


class IntegrityCheckStrategy(SelfLoggerMixin, ABC, Generic[TargetT]):
    """One named check: looks at a part of the storage and repairs what it has marked as repairable.

    ``TargetT`` is what the check looks at: the storage, a collection or one array.

    :param client: client of the storage
    """

    check: ClassVar[IntegrityCheck]

    def __init__(self, client: "Client") -> None:
        self.client = client
        self.root_path = client.root_path
        self.config = client._ctx.config

    @abstractmethod
    def run(self, target: TargetT) -> list[IntegrityFinding]:
        """Check the target.

        :param target: what to check
        :returns: the findings, empty if everything is fine
        """
        pass

    def repair(self, finding: IntegrityFinding) -> None:
        """Repair a finding of this check.

        :param finding: a repairable finding made by ``run``
        :raises DekerIntegrityError: if the kind of the finding cannot be repaired
        """
        self.refuse(finding)

    @staticmethod
    def refuse(finding: IntegrityFinding) -> NoReturn:
        """Refuse to repair a finding.

        :param finding: the finding
        :raises DekerIntegrityError: always
        """
        raise DekerIntegrityError(f"{finding.kind.value} cannot be repaired")

    def finding(
        self,
        kind: IntegrityFindingKind,
        message: str,
        collection: str | None = None,
        array_id: str | None = None,
        path: Path | None = None,
        repairable: bool = False,
    ) -> IntegrityFinding:
        """Make a finding of this check.

        :param kind: what was found
        :param message: the problem in words
        :param collection: name of the collection
        :param array_id: id of the array
        :param path: the file or directory the finding is about
        :param repairable: whether this check can repair it
        :returns: the finding
        """
        return IntegrityFinding(
            check=self.check,
            kind=kind,
            message=message,
            collection=collection,
            array_id=array_id,
            path=path,
            repairable=repairable,
        )

    def collection_of(self, finding: IntegrityFinding) -> Collection:
        """Get the collection a finding is about.

        :param finding: the finding
        :returns: the collection
        """
        if finding.collection is None:  # pragma: no cover  # guard: the checks always name the collection
            raise DekerIntegrityError(f"Finding {finding.kind.value} names no collection")
        collection = self.client.get_collection(finding.collection)
        if collection is None:  # pragma: no cover  # guard: the collection was there when the finding was made
            raise DekerCollectionNotExistsError(f"Collection {finding.collection} does not exist at this storage")
        return collection


class StorageCheck(IntegrityCheckStrategy[StorageFiles], ABC):
    """A check of the storage as a whole."""


class CollectionCheck(IntegrityCheckStrategy[CollectionFiles], ABC):
    """A check of one collection."""


class ArrayCheck(IntegrityCheckStrategy[ArrayOnDisk], ABC):
    """A check of one Array or VArray."""


def dict_diff(old: dict, new: dict, prefix: str = "") -> list[str]:
    """Tell what changes from one nested dict to another, field by field.

    :param old: the dict as it is
    :param new: the dict as it should be
    :param prefix: path of the dicts inside their parents, for the field names
    :returns: lines: ``+ path: value`` for an added field, ``- path: value`` for a removed one,
        ``~ path: old -> new`` for a changed one
    """
    lines = []
    for key in list(old) + [key for key in new if key not in old]:
        path = f"{prefix}.{key}" if prefix else str(key)
        if key not in new:
            lines.append(f"- {path}: {old[key]!r}")
        elif key not in old:
            lines.append(f"+ {path}: {new[key]!r}")
        else:
            lines.extend(_value_diff(old[key], new[key], path))
    return lines


def _value_diff(old: object, new: object, path: str) -> list[str]:
    """Tell what changes from one value to another; dicts and lists are compared element by element.

    :param old: the value as it is
    :param new: the value as it should be
    :param path: path of the value, for the field names
    :returns: the lines of the difference, see ``dict_diff``
    """
    if isinstance(old, dict) and isinstance(new, dict):
        return dict_diff(old, new, path)
    if isinstance(old, list) and isinstance(new, list):
        lines = []
        for n, (old_item, new_item) in enumerate(zip(old, new)):
            lines.extend(_value_diff(old_item, new_item, f"{path}[{n}]"))
        lines.extend(f"- {path}[{n}]: {item!r}" for n, item in enumerate(old) if n >= len(new))
        lines.extend(f"+ {path}[{n}]: {item!r}" for n, item in enumerate(new) if n >= len(old))
        return lines
    if old != new:
        return [f"~ {path}: {old!r} -> {new!r}"]
    return []


def remove_with_empty_parents(path: Path, stop: Path) -> None:
    """Remove a file or a symlink and the directories above it while they are empty.

    :param path: the file to remove
    :param stop: the directory to stop at, which is never removed
    """
    path.unlink()
    for parent in path.parents:  # pragma: no branch  # the loop always ends at the stop directory
        if parent == stop or os.listdir(parent):
            break
        parent.rmdir()


class CollectionsCheck(StorageCheck):
    """Every collection initialises and has its lock file; every lock file has its collection.

    A collection file of an older metadata version is reported: it opens with the defaults of the current
    version and is never rewritten.
    """

    check = IntegrityCheck.collections

    def run(self, target: StorageFiles) -> list[IntegrityFinding]:
        """Check the collections of the storage.

        :param target: the storage
        :returns: the findings
        """
        findings = []
        file_ext = self.client._adapter.file_ext
        for name, error in target.unreadable.items():
            findings.append(
                self.finding(
                    IntegrityFindingKind.collection_unreadable,
                    f"Collection {name} cannot be initialised: {error}",
                    collection=name,
                    path=target.root / name / (name + file_ext),
                )
            )
        # the file is the json of as_dict: the same round trip makes the two comparable field by field
        current = {collection.name: json.loads(json.dumps(collection.as_dict)) for collection in target.collections}
        for name, (version, stored) in target.outdated.items():
            changes = "\n".join(f"  {line}" for line in dict_diff(stored, current[name]))
            findings.append(
                self.finding(
                    IntegrityFindingKind.collection_outdated,
                    f"Collection {name} metadata version is {version}, the current one is "
                    f"{self.client.meta_version}; the file differs from the current format:\n{changes}",
                    collection=name,
                    path=target.root / name / (name + file_ext),
                )
            )
        for collection in target.collections:
            if collection.name not in target.lock_names:
                findings.append(
                    self.finding(
                        IntegrityFindingKind.lock_missing,
                        f"Lock file of collection {collection.name} not found",
                        collection=collection.name,
                        path=target.root / (collection.name + LocksExtensions.collection_lock.value),
                        repairable=True,
                    )
                )
        for name in target.lock_names:
            if name not in target.names:
                lock_file = target.root / (name + LocksExtensions.collection_lock.value)
                findings.append(
                    self.finding(
                        IntegrityFindingKind.lock_orphaned,
                        f"Lock file {lock_file.name} has no collection",
                        collection=name,
                        path=lock_file,
                        repairable=True,
                    )
                )
        return findings

    def repair(self, finding: IntegrityFinding) -> None:
        """Create the missing lock file or remove the orphaned one.

        :param finding: the finding
        """
        if finding.path is None:  # pragma: no cover  # guard: run() sets the path of every finding
            raise DekerIntegrityError(f"Finding {finding.kind.value} names no file")
        if finding.kind == IntegrityFindingKind.lock_missing:
            finding.path.touch()
        elif finding.kind == IntegrityFindingKind.lock_orphaned:
            finding.path.unlink()
        else:
            self.refuse(finding)


class MarkersCheck(CollectionCheck):
    """No markers of VArray writes are left by Deker < 2."""

    # TODO: remove in 3.0.0 with Client.clear_locks: the markers are not made since 2.0.0
    check = IntegrityCheck.markers

    def run(self, target: CollectionFiles) -> list[IntegrityFinding]:
        """Look for the markers in the collection.

        :param target: the collection
        :returns: the findings
        """
        return [
            self.finding(
                IntegrityFindingKind.marker_left,
                f"Marker {marker.name} is left by a VArray write of an older version",
                collection=target.collection.name,
                path=marker,
                repairable=True,
            )
            for marker in sorted(target.path.rglob(f"*{LocksExtensions.varray_lock.value}"))
        ]

    def repair(self, finding: IntegrityFinding) -> None:
        """Remove the marker.

        :param finding: the finding
        """
        if finding.kind != IntegrityFindingKind.marker_left or finding.path is None:
            self.refuse(finding)
        finding.path.unlink()


class ArraysCheck(ArrayCheck):
    """Every main file initialises an Array or a VArray.

    An ``Array`` of a VArray collection also refers to an existing ``VArray``, has its ``v_position`` inside
    the ``vgrid`` and carries the time attributes of its ``v_position``.
    """

    check = IntegrityCheck.arrays

    def _check_varray_of(self, target: ArrayOnDisk) -> list[IntegrityFinding]:
        """Check an Array of a VArray collection against its VArray; a VArray and an Array without one pass.

        :param target: the array
        :returns: the findings
        """
        array = target.array
        if not isinstance(array, Array) or array._vid is None:
            return []
        if array._vid in target.files.unreadable_varray_ids:
            return []  # the VArray is there but unreadable: reported for its own file
        name = target.files.collection.name
        varray = target.files.varrays_by_id.get(array._vid)
        if varray is None:
            return [
                self.finding(
                    IntegrityFindingKind.varray_missing,
                    f"Array {array.id} refers to a missing VArray {array._vid}",
                    collection=name,
                    array_id=array.id,
                    path=target.main_file,
                )
            ]
        vposition = tuple(array._v_position or ())
        vgrid = tuple(varray.vgrid)
        if len(vposition) != len(vgrid) or any(not 0 <= p < g for p, g in zip(vposition, vgrid)):
            return [
                self.finding(
                    IntegrityFindingKind.position_out_of_grid,
                    f"Array {array.id} v_position {vposition} is outside vgrid {vgrid} of VArray {varray.id}",
                    collection=name,
                    array_id=array.id,
                    path=target.main_file,
                )
            ]
        findings = []
        expected = array_time_attributes(varray, vposition, target.files.collection.array_schema)
        for attribute, value in expected.items():
            actual = array.custom_attributes.get(attribute)
            if actual != value:
                findings.append(
                    self.finding(
                        IntegrityFindingKind.time_attribute_wrong,
                        f"Array {array.id} attribute {attribute} is {actual}, expected {value.isoformat()}",
                        collection=name,
                        array_id=array.id,
                        path=target.main_file,
                        repairable=True,
                    )
                )
        return findings

    def run(self, target: ArrayOnDisk) -> list[IntegrityFinding]:
        """Report the file if its array did not initialise, else check the Array against its VArray.

        :param target: the array
        :returns: the findings
        """
        if target.array is None:
            return [
                self.finding(
                    IntegrityFindingKind.meta_unreadable,
                    f"{target.kind} {target.array_id} cannot be initialised: {target.error}",
                    collection=target.files.collection.name,
                    array_id=target.array_id,
                    path=target.main_file,
                )
            ]
        return self._check_varray_of(target)

    def repair(self, finding: IntegrityFinding) -> None:
        """Write the time attributes calculated from the v_position of the Array.

        :param finding: the finding
        """
        if finding.kind != IntegrityFindingKind.time_attribute_wrong or finding.array_id is None:
            self.refuse(finding)
        collection = self.collection_of(finding)
        array = collection.arrays.filter({"id": finding.array_id}).last()
        if not isinstance(array, Array) or array._vid is None:  # pragma: no cover  # guard: it was an Array
            raise DekerIntegrityError(f"Array {finding.array_id} is not an Array of a VArray")
        varray = collection.varrays.filter({"id": array._vid}).last()
        if not isinstance(varray, VArray):  # pragma: no cover  # guard: the VArray was there at check time
            raise DekerIntegrityError(f"VArray {array._vid} of Array {array.id} does not exist")
        array.update_custom_attributes(
            array_time_attributes(varray, tuple(array._v_position or ()), collection.array_schema)
        )


class LinksCheck(CollectionCheck):
    """Main files and symlinks point at each other, one array per directory of primary attributes."""

    check = IntegrityCheck.links

    def expected_symlink(self, entry: ArrayOnDisk) -> Path:
        """The symlink an array shall have, by its primary attributes.

        :param entry: the array
        :returns: path of the symlink
        """
        if entry.array is None:  # pragma: no cover  # guard: called for the arrays that initialised only
            raise DekerIntegrityError(f"{entry.kind} {entry.array_id} is not initialised")
        collection = entry.files.collection
        if isinstance(entry.array, VArray):
            schema = collection.varray_schema
            symlinks_dir = self.config.varray_symlinks_directory
            attributes = dict(entry.array.primary_attributes)
        else:
            schema = collection.array_schema
            symlinks_dir = self.config.array_symlinks_directory
            attributes = {
                **entry.array.primary_attributes,
                "vid": entry.array._vid,
                "v_position": entry.array._v_position,
            }
        if schema is None:  # pragma: no cover  # guard: a VArray lives in a collection with a VArray schema
            raise DekerIntegrityError(f"Collection {collection.name} has no schema for {entry.kind} {entry.array_id}")
        directory = get_symlink_path(entry.files.path / symlinks_dir, schema.primary_attributes, attributes)
        return directory / entry.main_file.name

    def _check_group(
        self, target: CollectionFiles, entries: list[ArrayOnDisk], symlinks: list[Path], symlinks_root: Path
    ) -> list[IntegrityFinding]:
        """Check the Arrays or the VArrays of a collection against their symlinks.

        :param target: the collection
        :param entries: the main files
        :param symlinks: what lies under the symlinks directory
        :param symlinks_root: the symlinks directory
        :returns: the findings
        """
        name = target.collection.name
        findings = []
        expected = {self.expected_symlink(entry): entry for entry in entries if entry.array is not None}
        # the symlinks of the arrays that did not initialise cannot be placed: the arrays check reports them
        unreadable = {entry.main_file.resolve() for entry in entries if entry.array is None}
        present: set[Path] = set()
        valid_by_dir: dict[Path, list[Path]] = {}
        for symlink in sorted(symlinks):
            array_id = symlink.stem
            if not symlink.is_symlink():
                findings.append(
                    self.finding(
                        IntegrityFindingKind.stray_file,
                        f"File {symlink} is not a symlink of an array",
                        collection=name,
                        path=symlink,
                    )
                )
                continue
            if not symlink.exists():
                findings.append(
                    self.finding(
                        IntegrityFindingKind.symlink_broken,
                        f"Symlink {symlink} points to a missing file {os.readlink(symlink)}",
                        collection=name,
                        array_id=array_id,
                        path=symlink,
                        repairable=True,
                    )
                )
                continue
            if symlink.resolve() in unreadable:
                continue
            entry = expected.get(symlink)
            if entry is None or symlink.resolve() != entry.main_file.resolve():
                findings.append(
                    self.finding(
                        IntegrityFindingKind.symlink_misplaced,
                        f"Symlink {symlink} is not the symlink of {array_id} by its primary attributes",
                        collection=name,
                        array_id=array_id,
                        path=symlink,
                        repairable=True,
                    )
                )
                continue
            present.add(symlink)
            if symlink.parent != symlinks_root:
                valid_by_dir.setdefault(symlink.parent, []).append(symlink)
        for symlink, entry in expected.items():
            if symlink not in present:
                findings.append(
                    self.finding(
                        IntegrityFindingKind.symlink_missing,
                        f"Symlink {symlink} of {entry.kind} {entry.array_id} not found",
                        collection=name,
                        array_id=entry.array_id,
                        path=symlink,
                        repairable=True,
                    )
                )
        for directory, links in valid_by_dir.items():
            if len(links) > 1:
                ids = ", ".join(link.stem for link in links)
                findings.append(
                    self.finding(
                        IntegrityFindingKind.duplicate_arrays,
                        f"Directory {directory} holds several arrays with the same primary attributes: {ids}",
                        collection=name,
                        path=directory,
                    )
                )
        return findings

    def run(self, target: CollectionFiles) -> list[IntegrityFinding]:
        """Check the links of the collection in both directions.

        :param target: the collection
        :returns: the findings
        """
        findings = self._check_group(
            target, target.arrays, target.array_symlinks, target.path / self.config.array_symlinks_directory
        )
        if target.collection.varray_schema:
            findings.extend(
                self._check_group(
                    target, target.varrays, target.varray_symlinks, target.path / self.config.varray_symlinks_directory
                )
            )
        return findings

    def repair(self, finding: IntegrityFinding) -> None:
        """Create the missing symlink or remove the broken or misplaced one with the empty directories above.

        :param finding: the finding
        """
        repairable = (
            IntegrityFindingKind.symlink_missing,
            IntegrityFindingKind.symlink_broken,
            IntegrityFindingKind.symlink_misplaced,
        )
        if finding.kind not in repairable or finding.path is None or finding.array_id is None:
            self.refuse(finding)
        collection = self.collection_of(finding)
        path = collection.path
        if not isinstance(path, Path):  # pragma: no cover  # guard: the checker works on local storages only
            raise DekerIntegrityError(f"Collection {collection.name} is not on a local disk: {path}")
        is_varray = finding.path.is_relative_to(path / self.config.varray_symlinks_directory)
        if finding.kind == IntegrityFindingKind.symlink_missing:
            data_dir = self.config.varray_data_directory if is_varray else self.config.array_data_directory
            main_file = get_main_path(finding.array_id, path / data_dir) / finding.path.name
            finding.path.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(main_file, finding.path)
            return
        symlinks_root = path / (
            self.config.varray_symlinks_directory if is_varray else self.config.array_symlinks_directory
        )
        remove_with_empty_parents(finding.path, symlinks_root)


class DataCheck(ArrayCheck):
    """One cell of every Array reads back with the dtype of the collection."""

    check = IntegrityCheck.data

    def run(self, target: ArrayOnDisk) -> list[IntegrityFinding]:
        """Read the last cell of the Array; a VArray is read through its Arrays, which are checked themselves.

        :param target: the array
        :returns: the findings
        """
        array = target.array
        if not isinstance(array, Array):
            return []
        name = target.files.collection.name
        try:
            data = array[tuple(size - 1 for size in array.shape)].read()
        except Exception as e:  # noqa: BLE001  # whatever stops the read is the finding
            return [
                self.finding(
                    IntegrityFindingKind.data_unreadable,
                    f"Array {array.id} data is corrupted: {e!s}",
                    collection=name,
                    array_id=array.id,
                    path=target.main_file,
                )
            ]
        if np.asarray(data).dtype != array.dtype:
            return [
                self.finding(
                    IntegrityFindingKind.dtype_mismatch,
                    f"Array {array.id} data is corrupted: incorrect dtype {np.asarray(data).dtype}",
                    collection=name,
                    array_id=array.id,
                    path=target.main_file,
                )
            ]
        return []


class CountersCheck(ArrayCheck):
    """The counter of empty cells of every Array equals the number of fill values in its data."""

    check = IntegrityCheck.counters

    def run(self, target: ArrayOnDisk) -> list[IntegrityFinding]:
        """Compare the counter with the data of the Array.

        :param target: the array
        :returns: the findings
        """
        array = target.array
        if not isinstance(array, Array):
            return []
        storage = array._adapter.storage_adapter
        stored = storage.read_empty_cells(target.main_file)
        counted = storage.count_empty_cells(target.main_file, array.shape, array.fill_value)
        if stored == counted:
            return []
        return [
            self.finding(
                IntegrityFindingKind.counter_drifted,
                f"Array {array.id} empty cells counter is {stored}, counted {counted}",
                collection=target.files.collection.name,
                array_id=array.id,
                path=target.main_file,
                repairable=True,
            )
        ]

    def repair(self, finding: IntegrityFinding) -> None:
        """Count the empty cells again and store the number, holding the file exclusively.

        :param finding: the finding
        """
        if finding.kind != IntegrityFindingKind.counter_drifted or finding.array_id is None or finding.path is None:
            self.refuse(finding)
        collection = self.collection_of(finding)
        array = collection.arrays.filter({"id": finding.array_id}).last()
        if not isinstance(array, Array):  # pragma: no cover  # guard: the array was there at check time
            raise DekerIntegrityError(f"Array {finding.array_id} does not exist")
        storage = array._adapter.storage_adapter
        with Flock(finding.path):
            counted = storage.count_empty_cells(finding.path, array.shape, array.fill_value)
            storage.write_empty_cells(finding.path, counted)


INTEGRITY_CHECKS: dict[IntegrityCheck, type[IntegrityCheckStrategy]] = {
    strategy.check: strategy
    for strategy in (
        CollectionsCheck,
        MarkersCheck,
        ArraysCheck,
        LinksCheck,
        DataCheck,
        CountersCheck,
    )
}


class IntegrityChecker(SelfLoggerMixin):
    """Storage integrity checker: walks the storage and runs the selected checks on what it finds.

    :param client: client of the storage
    :param checks: the checks to run, in any order; they run from the cheapest to the most expensive
    :param stop_on_error: raise ``DekerIntegrityError`` at the first finding instead of collecting them
    :param repair: repair the repairable findings of every collection once it is checked
    :param workers: threads to check the arrays of a collection with
    """

    def __init__(
        self,
        client: "Client",
        checks: Iterable[IntegrityCheck] = DEFAULT_INTEGRITY_CHECKS,
        stop_on_error: bool = False,
        repair: bool = False,
        workers: int = 1,
    ) -> None:
        selected = set(checks)
        for check in selected:
            if not isinstance(check, IntegrityCheck):
                raise ValueError(f"{check!r} is not an IntegrityCheck")
        if workers < 1:
            raise ValueError(f"workers shall be a positive number, got {workers}")
        self.client = client
        self.root_path = client.root_path
        self.config = client._ctx.config
        self.stop_on_error = stop_on_error
        self.repair = repair
        self.workers = workers
        self.strategies: dict[IntegrityCheck, IntegrityCheckStrategy] = {
            check: INTEGRITY_CHECKS[check](client) for check in IntegrityCheck if check in selected
        }
        self.storage_checks = [s for s in self.strategies.values() if isinstance(s, StorageCheck)]
        self.collection_checks = [s for s in self.strategies.values() if isinstance(s, CollectionCheck)]
        self.array_checks = [s for s in self.strategies.values() if isinstance(s, ArrayCheck)]

    def _add(self, report: IntegrityReport, findings: list[IntegrityFinding]) -> None:
        """Put the findings into the report, stopping if told to.

        :param report: the report
        :param findings: the new findings
        :raises DekerIntegrityError: at the first finding if the checker stops on errors
        """
        report.findings.extend(findings)
        if self.stop_on_error and findings:
            raise DekerIntegrityError(str(report))

    def _repair(self, findings: list[IntegrityFinding]) -> None:
        """Repair the repairable findings, each by the check that made it.

        :param findings: the findings
        """
        if not self.repair:
            return
        for finding in findings:
            if not finding.repairable:
                continue
            try:
                self.strategies[finding.check].repair(finding)
            except Exception as e:  # noqa: BLE001  # a failed repair is reported, the others go on
                finding.repair_error = str(e)
                self.logger.warning("Repair of %s failed: %s", finding.kind.value, e)
            else:
                finding.repaired = True
                self.logger.info("Repaired: %s", finding.message)

    def _init_collection(self, name: str, storage: StorageFiles, named: bool) -> None:
        """Initialise a collection into the storage files, keeping the reason if it fails.

        :param name: name of the collection
        :param storage: the storage files
        :param named: the collection was asked for by name; otherwise its directory was found in the storage
        :raises DekerCollectionNotExistsError: if the collection asked for by name does not exist
        """
        try:
            stored = self.client._adapter.read(name)
            collection = self.client.get_collection(name)
        except DekerCollectionNotExistsError:
            if named:
                raise DekerCollectionNotExistsError(f"Collection {name} does not exist at this storage") from None
            storage.unreadable[name] = "the directory has no collection file"
            return
        except (DekerBaseApplicationError, ValueError) as e:
            storage.unreadable[name] = str(e)
            return
        if collection is None:  # pragma: no cover  # guard: the file was read a moment ago
            raise DekerCollectionNotExistsError(f"Collection {name} does not exist at this storage")
        version = stored.get("metadata_version")
        if version is not None and version < self.client.meta_version:
            # opened with the defaults of the current version, never rewritten
            storage.outdated[name] = (version, stored)
        storage.collections.append(collection)

    def _scan_storage(self, collection_name: str | None) -> StorageFiles:
        """Look at the root of the storage: the collections and their lock files.

        :param collection_name: the only collection to look at, every collection if ``None``
        :returns: the storage files
        """
        storage = StorageFiles(root=self.root_path)
        lock_ext = LocksExtensions.collection_lock.value
        if collection_name is not None:
            storage.names.append(collection_name)
            if (self.root_path / (collection_name + lock_ext)).is_file():
                storage.lock_names.append(collection_name)
        else:
            for entry in sorted(self.root_path.iterdir()):
                if entry.is_dir():
                    storage.names.append(entry.name)
                elif entry.name.endswith(lock_ext):
                    storage.lock_names.append(entry.name[: -len(lock_ext)])
        for name in storage.names:
            self._init_collection(name, storage, named=collection_name is not None)
        return storage

    def _read_array(self, entry: ArrayOnDisk) -> ArrayOnDisk:
        """Initialise the array of a main file, keeping the reason if it fails.

        :param entry: the main file
        :returns: the same entry with the array or the error set
        """
        collection = entry.files.collection
        manager = collection.varrays if entry.is_varray else collection.arrays
        try:
            entry.array = manager.filter({"id": entry.array_id}).last()
        except Exception as e:  # noqa: BLE001  # whatever stops the initialisation is the finding
            entry.error = str(e)
        if entry.array is None and not entry.error:  # pragma: no cover  # guard: the file is there, so is the array
            entry.error = "the main file is not found by the id"
        return entry

    def _scan_collection(self, collection: Collection, pool: ThreadPoolExecutor) -> CollectionFiles:
        """Look at the disk of a collection: the main files, read into arrays, and the symlinks.

        :param collection: the collection
        :param pool: threads to read the arrays with
        :returns: the collection files
        """
        files = CollectionFiles(collection=collection)
        path = files.path
        array_ext = collection._storage_adapter.file_ext
        entries = [
            ArrayOnDisk(files, main_file, is_varray=False)
            for main_file in sorted((path / self.config.array_data_directory).rglob(f"*{array_ext}"))
        ]
        if collection.varray_schema:
            # circular import otherwise
            from deker_local_adapters.varray_adapter import LocalVArrayAdapter

            varray_adapter = self.client._factory.get_varray_adapter(path, storage_adapter=collection._storage_adapter)
            if not isinstance(varray_adapter, LocalVArrayAdapter):  # pragma: no cover  # guard: local storages only
                raise DekerIntegrityError(f"Collection {collection.name} is not on a local disk: {path}")
            entries.extend(
                ArrayOnDisk(files, main_file, is_varray=True)
                for main_file in sorted((path / self.config.varray_data_directory).rglob(f"*{varray_adapter.file_ext}"))
            )
        for entry in pool.map(self._read_array, entries):
            (files.varrays if entry.is_varray else files.arrays).append(entry)
        files.array_symlinks = [
            entry for entry in (path / self.config.array_symlinks_directory).rglob("*") if not entry.is_dir()
        ]
        files.varray_symlinks = [
            entry for entry in (path / self.config.varray_symlinks_directory).rglob("*") if not entry.is_dir()
        ]
        return files

    def _check_array(self, entry: ArrayOnDisk) -> list[IntegrityFinding]:
        """Run the array checks on one array.

        :param entry: the array
        :returns: the findings
        """
        if entry.array is None:  # pragma: no cover  # guard: only the arrays that initialised are submitted
            return []
        findings = []
        for strategy in self.array_checks:
            findings.extend(strategy.run(entry))
        return findings

    def _check_collection(self, collection: Collection, pool: ThreadPoolExecutor, report: IntegrityReport) -> None:
        """Run the collection and array checks on one collection and repair its findings.

        :param collection: the collection
        :param pool: threads to check the arrays with
        :param report: the report to put the findings into
        """
        first = len(report.findings)
        files = self._scan_collection(collection, pool)
        for strategy in self.collection_checks:
            self._add(report, strategy.run(files))
        entries = files.arrays + files.varrays
        unreadable = [entry for entry in entries if entry.array is None]
        if IntegrityCheck.arrays in self.strategies:
            for entry in unreadable:
                self._add(report, self.strategies[IntegrityCheck.arrays].run(entry))
        if self.array_checks:
            futures: list[Future[list[IntegrityFinding]]] = [
                pool.submit(self._check_array, entry) for entry in entries if entry.array is not None
            ]
            progress = tqdm.tqdm(total=len(futures), desc=f"Collection {collection.name}", leave=False)
            try:
                for future in as_completed(futures):
                    self._add(report, future.result())
                    progress.update()
            finally:
                progress.close()
        self._repair(report.findings[first:])

    def check(self, collection: str | None = None) -> IntegrityReport:
        """Run the checks.

        :param collection: name of the only collection to check, every collection if ``None``
        :returns: the report
        :raises DekerCollectionNotExistsError: if there is no such collection
        :raises DekerIntegrityError: at the first finding if the checker stops on errors
        """
        self.logger.info("Integrity check is running...")
        report = IntegrityReport()
        storage = self._scan_storage(collection)
        for strategy in self.storage_checks:
            self._add(report, strategy.run(storage))
        self._repair(report.findings)
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            collections = tqdm.tqdm(storage.collections, desc="Integrity check", leave=False)
            for item in collections:
                self.logger.info("Integrity check of collection %s", item.name)
                self._check_collection(item, pool, report)
        return report
