"""Tests of the integrity checker: the walker, every check, the repairs and the report."""

import json
import os
import shutil

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import h5py
import numpy as np
import pytest

from cantok import DefaultToken
from deker_tools.typings import Sentinel

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerCollectionNotExistsError,
    DekerCriticalError,
    DekerIntegrityError,
    DekerLockError,
    DekerTrashError,
)
from deker.flock import Flock
from deker.integrity import (
    INTEGRITY_CHECKS,
    ArraysCheck,
    CollectionsCheck,
    CountersCheck,
    DataCheck,
    IntegrityChecker,
    IntegrityCheckStrategy,
    LinksCheck,
    MarkersCheck,
    dict_diff,
)
from deker.tools import get_main_path, get_symlink_path
from deker.types import (
    DEFAULT_INTEGRITY_CHECKS,
    IntegrityCheck,
    IntegrityFinding,
    IntegrityFindingKind,
    IntegrityReport,
    LocksExtensions,
)


def main_file(array: Array | VArray) -> Path:
    """Main file of an Array or a VArray.

    :param array: the array
    :returns: path to its main file
    """
    adapter = array._adapter
    return get_main_path(array.id, adapter.collection_path / adapter.data_dir) / (array.id + adapter.file_ext)


def symlink_dir(array: Array | VArray) -> Path:
    """Directory of the symlink of an Array or a VArray, by its primary attributes.

    :param array: the array
    :returns: path to the directory
    """
    collection = array._Array__collection if isinstance(array, Array) else array._VArray__collection
    schema = collection.array_schema if isinstance(array, Array) else collection.varray_schema
    attributes = dict(array.primary_attributes)
    if isinstance(array, Array):
        attributes.update({"vid": array._vid, "v_position": array._v_position})
    return get_symlink_path(collection.path / array._adapter.symlinks_dir, schema.primary_attributes, attributes)


def symlink_of(array: Array | VArray) -> Path:
    """Symlink of an Array or a VArray.

    :param array: the array
    :returns: path to its symlink
    """
    return symlink_dir(array) / (array.id + array._adapter.file_ext)


def rewrite_meta(array: Array | VArray, changes: dict[str, Any]) -> None:
    """Change the metadata of an Array or a VArray on the disk.

    :param array: the array
    :param changes: top-level keys of the metadata to replace
    """
    file = main_file(array)
    if isinstance(array, VArray):
        meta = json.loads(file.read_text())
        meta.update(changes)
        file.write_text(json.dumps(meta, default=str))
        return
    with h5py.File(file, "r+") as f:
        meta = json.loads(f["meta"][()])
        meta.update(changes)
        del f["meta"]
        payload = json.dumps(meta, default=str)
        f.create_dataset("meta", dtype=f"S{len(payload.encode())}", shape=(), data=payload)


def kinds(report: IntegrityReport) -> list[tuple[IntegrityCheck, IntegrityFindingKind]]:
    """Checks and kinds of the findings of a report, in order.

    :param report: the report
    :returns: pairs of check and kind
    """
    return [(finding.check, finding.kind) for finding in report.findings]


def files_of(path: Path) -> dict[Path, float]:
    """Every file under a path with its modification time.

    :param path: the directory
    :returns: files and their mtimes
    """
    return {file: file.lstat().st_mtime for file in path.rglob("*")}


def checker(client: Client, *checks: IntegrityCheck, **options: Any) -> IntegrityChecker:
    """Make a checker with the given checks.

    :param client: the client
    :param checks: checks to run, the default set if none
    :param options: options of the checker
    :returns: the checker
    """
    return IntegrityChecker(client, checks=checks or DEFAULT_INTEGRITY_CHECKS, **options)


class TestDictDiff:
    """The difference between two nested dicts, as lines of added, removed and changed fields."""

    @pytest.mark.parametrize(
        ("old", "new", "expected"),
        [
            ({}, {}, []),
            ({"a": 1}, {"a": 1}, []),
            ({}, {"a": 1}, ["+ a: 1"]),
            ({"a": 1}, {}, ["- a: 1"]),
            ({"a": 1}, {"a": 2}, ["~ a: 1 -> 2"]),
            ({"a": {"b": 1}}, {"a": {"b": 1, "c": None}}, ["+ a.c: None"]),
            ({"a": {"b": 1}}, {"a": {"b": 2}}, ["~ a.b: 1 -> 2"]),
            ({"a": [1, 2]}, {"a": [1, 3]}, ["~ a[1]: 2 -> 3"]),
            ({"a": [1]}, {"a": [1, 2]}, ["+ a[1]: 2"]),
            ({"a": [1, 2]}, {"a": [1]}, ["- a[1]: 2"]),
            ({"a": [{"b": 1}]}, {"a": [{"b": 1, "c": 2}]}, ["+ a[0].c: 2"]),
            ({"a": {"b": 1}}, {"a": 1}, ["~ a: {'b': 1} -> 1"]),
            ({"a": [1]}, {"a": {"b": 1}}, ["~ a: [1] -> {'b': 1}"]),
            ({"a": "x", "b": 2}, {"a": "y", "c": 3}, ["~ a: 'x' -> 'y'", "- b: 2", "+ c: 3"]),
        ],
    )
    def test_diff(self, old: dict, new: dict, expected: list[str]):
        assert dict_diff(old, new) == expected


class TestIntegrityCheckSets:
    """The named checks and their default set."""

    def test_default_checks_read_no_data(self):
        assert DEFAULT_INTEGRITY_CHECKS == (
            IntegrityCheck.collections,
            IntegrityCheck.markers,
            IntegrityCheck.arrays,
            IntegrityCheck.links,
        )

    def test_every_check_is_registered(self):
        assert set(INTEGRITY_CHECKS) == set(IntegrityCheck)
        for check, strategy in INTEGRITY_CHECKS.items():
            assert issubclass(strategy, IntegrityCheckStrategy)
            assert strategy.check is check

    @pytest.mark.parametrize(
        ("check", "strategy"),
        [
            (IntegrityCheck.collections, CollectionsCheck),
            (IntegrityCheck.markers, MarkersCheck),
            (IntegrityCheck.arrays, ArraysCheck),
            (IntegrityCheck.links, LinksCheck),
            (IntegrityCheck.data, DataCheck),
            (IntegrityCheck.counters, CountersCheck),
        ],
    )
    def test_registry(self, check: IntegrityCheck, strategy: type[IntegrityCheckStrategy]):
        assert INTEGRITY_CHECKS[check] is strategy

    def test_checker_rejects_an_unknown_check(self, client: Client):
        with pytest.raises(ValueError, match="not an IntegrityCheck"):
            IntegrityChecker(client, checks=["links"])  # type: ignore[list-item]

    @pytest.mark.parametrize("threads", [0, -1])
    def test_checker_rejects_bad_workers(self, client: Client, threads: int):
        with pytest.raises(ValueError, match="workers"):
            IntegrityChecker(client, workers=threads)


class TestCollectionsCheck:
    """Every collection initialises and has its lock file; every lock file has its collection."""

    def test_ok(self, client: Client, array_collection: Collection, varray_collection: Collection):
        report = checker(client, IntegrityCheck.collections).check()
        assert report.ok

    def test_unreadable_collection_is_reported_and_not_checked_further(
        self, client: Client, array_collection: Collection, inserted_array: Array
    ):
        meta_file = array_collection.path / (array_collection.name + ".json")
        meta_file.write_text("{not json")
        try:
            report = checker(client, IntegrityCheck.collections, IntegrityCheck.links).check()
            assert kinds(report) == [(IntegrityCheck.collections, IntegrityFindingKind.collection_unreadable)]
            finding = report.findings[0]
            assert finding.collection == array_collection.name
            assert finding.path == meta_file
            assert not finding.repairable
            assert array_collection.name in finding.message
        finally:
            meta_file.write_text(json.dumps(array_collection.as_dict))

    def test_collection_with_broken_schema_is_reported(self, client: Client, array_collection: Collection):
        meta_file = array_collection.path / (array_collection.name + ".json")
        meta = json.loads(meta_file.read_text())
        meta["schema"]["dtype"] = "test"
        meta_file.write_text(json.dumps(meta))
        try:
            report = checker(client, IntegrityCheck.collections).check(array_collection.name)
            assert kinds(report) == [(IntegrityCheck.collections, IntegrityFindingKind.collection_unreadable)]
            assert "'test'" in report.findings[0].message
        finally:
            meta_file.write_text(json.dumps(array_collection.as_dict))

    @pytest.mark.parametrize("version", ["0.1", "0.0"])
    def test_outdated_metadata_version_is_reported_with_the_diff_and_not_repaired(
        self, client: Client, array_collection: Collection, version: str
    ):
        """A collection written by an older Deker still opens; the check tells what a rewrite would change."""
        meta_file = array_collection.path / (array_collection.name + ".json")
        meta = json.loads(meta_file.read_text())
        meta["metadata_version"] = version
        del meta["schema"]["dimensions"][0]["scale"]  # the older format had no scale
        meta["legacy"] = True  # a field the current format does not know
        meta_file.write_text(json.dumps(meta))
        try:
            report = checker(client, IntegrityCheck.collections).check(array_collection.name)
            assert kinds(report) == [(IntegrityCheck.collections, IntegrityFindingKind.collection_outdated)]
            finding = report.findings[0]
            assert finding.collection == array_collection.name
            assert finding.path == meta_file
            assert not finding.repairable
            lines = finding.message.split("\n")
            assert lines[0] == (
                f"Collection {array_collection.name} metadata version is {version}, the current one is "
                f"{client.meta_version}; the file differs from the current format:"
            )
            assert sorted(lines[1:]) == sorted(
                [
                    f"  ~ metadata_version: {version!r} -> {client.meta_version!r}",
                    "  + schema.dimensions[0].scale: None",
                    "  - legacy: True",
                ]
            )
            checker(client, IntegrityCheck.collections, repair=True).check(array_collection.name)
            assert json.loads(meta_file.read_text()) == meta  # informed, not rewritten
            assert client.get_collection(array_collection.name) is not None
        finally:
            meta_file.write_text(json.dumps(array_collection.as_dict))

    def test_outdated_metadata_version_with_nothing_else_to_change(self, client: Client, array_collection: Collection):
        """Only the version differs: the diff names it alone."""
        meta_file = array_collection.path / (array_collection.name + ".json")
        meta = json.loads(meta_file.read_text())
        meta["metadata_version"] = "0.1"
        meta_file.write_text(json.dumps(meta))
        try:
            report = checker(client, IntegrityCheck.collections).check(array_collection.name)
            assert report.findings[0].message.split("\n")[1:] == [
                f"  ~ metadata_version: '0.1' -> {client.meta_version!r}"
            ]
        finally:
            meta_file.write_text(json.dumps(array_collection.as_dict))

    def test_current_metadata_version_is_not_reported(self, client: Client, array_collection: Collection):
        meta_file = array_collection.path / (array_collection.name + ".json")
        assert json.loads(meta_file.read_text())["metadata_version"] == client.meta_version
        assert checker(client, IntegrityCheck.collections).check(array_collection.name).ok

    @pytest.mark.parametrize("empty_file", [False, True], ids=["no-file", "empty-file"])
    def test_directory_without_a_collection_file_is_reported(
        self, client: Client, array_collection: Collection, empty_file: bool
    ):
        """A directory in the storage root that holds no collection file, or an empty one (a creation that has not
        written it), is a collection that cannot be read.
        """
        stray = client.root_path / "half-made"
        stray.mkdir()
        if empty_file:
            (stray / ("half-made" + client._adapter.file_ext)).touch()
        try:
            report = checker(client, IntegrityCheck.collections).check()
            assert kinds(report) == [(IntegrityCheck.collections, IntegrityFindingKind.collection_unreadable)]
            finding = report.findings[0]
            assert finding.collection == "half-made"
            assert "no collection file" in finding.message
            assert finding.repairable
            with pytest.raises(DekerCollectionNotExistsError):
                checker(client, IntegrityCheck.collections).check("half-made")
        finally:
            shutil.rmtree(stray)

    @pytest.mark.parametrize("kind", ["empty-directory", "empty-file", "directory-with-arrays"])
    def test_directory_without_a_collection_file_is_removed_by_the_repair(
        self, client: Client, array_schema, mocker, kind: str
    ):
        """The directory is moved to the trash with everything in it, the files of the arrays too, and the trash is
        emptied; nothing outside the directory is touched.
        """
        name = "half_made"
        directory = client.root_path / name
        json_file = directory / (name + client._adapter.file_ext)
        trash = client._adapter.trash_resource
        assert list(trash.iterdir()) == []
        if kind == "directory-with-arrays":
            collection = client.create_collection(name, array_schema)
            array_file = main_file(collection.create())
            json_file.unlink()
            assert array_file.is_file()
        else:
            directory.mkdir()
            if kind == "empty-file":
                json_file.touch()
        others = {
            path: mtime
            for path, mtime in files_of(client.root_path).items()
            if path != directory and directory not in path.parents
        }
        spy = mocker.spy(client._adapter, "empty_trash")
        try:
            report = checker(client, IntegrityCheck.collections, repair=True).check()
            assert kinds(report) == [(IntegrityCheck.collections, IntegrityFindingKind.collection_unreadable)]
            assert report.findings[0].repaired
            assert report.findings[0].repair_error == ""
            assert not directory.exists()
            assert sorted(client.root_path.glob(f"{name}*")) == []  # no lock file is made
            assert checker(client, IntegrityCheck.collections).check().ok
            spy.assert_called_once_with(background=False)
            assert list(trash.iterdir()) == []  # emptied by the repair
            assert files_of(client.root_path) == others
        finally:
            shutil.rmtree(directory, ignore_errors=True)
            client.empty_trash()
        assert list(trash.iterdir()) == []

    def test_trash_error_of_the_repair_is_raised_and_reported(
        self, client: Client, array_collection: Collection, mocker
    ):
        """The directory is in the trash already when the trash is not emptied: the repair raises the trash error, the
        checker reports it as a failed repair.
        """
        directory = client.root_path / "half-made"
        directory.mkdir()
        trash = client._adapter.trash_resource
        error = DekerTrashError("The trash of the storage is not emptied")
        mocker.patch.object(client._adapter, "empty_trash", side_effect=error)
        try:
            finding = checker(client, IntegrityCheck.collections).check().findings[0]
            with pytest.raises(DekerTrashError) as info:
                CollectionsCheck(client).repair(finding)
            assert info.value is error
            assert not directory.exists()
            assert len(list(trash.iterdir())) == 1
            directory.mkdir()
            (repaired,) = checker(client, IntegrityCheck.collections, repair=True).check().findings
            assert not repaired.repaired
            assert repaired.repair_error == "The trash of the storage is not emptied"
        finally:
            mocker.stopall()
            shutil.rmtree(directory, ignore_errors=True)
            client.empty_trash()
        assert list(trash.iterdir()) == []

    def test_broken_collection_file_is_not_removed(self, client: Client, array_collection: Collection):
        """A broken collection file is not repairable; given to the repair anyway, it is refused (a guard)."""
        meta_file = array_collection.path / (array_collection.name + ".json")
        meta_file.write_text("{not json")
        try:
            report = checker(client, IntegrityCheck.collections, repair=True).check()
            assert kinds(report) == [(IntegrityCheck.collections, IntegrityFindingKind.collection_unreadable)]
            assert not report.findings[0].repaired
            with pytest.raises(DekerIntegrityError, match="cannot be repaired"):
                CollectionsCheck(client).repair(report.findings[0])
            assert meta_file.read_text() == "{not json"
        finally:
            meta_file.write_text(json.dumps(array_collection.as_dict))

    def test_collection_file_written_meanwhile_is_not_removed(self, client: Client, array_collection: Collection):
        """The creator wrote the collection file between the finding and the repair: the repair is refused (a guard)."""
        directory = client.root_path / "half-made"
        directory.mkdir()
        try:
            finding = checker(client, IntegrityCheck.collections).check().findings[0]
            (directory / ("half-made" + client._adapter.file_ext)).write_text("{}")
            with pytest.raises(DekerIntegrityError, match="cannot be repaired"):
                CollectionsCheck(client).repair(finding)
            assert (directory / ("half-made" + client._adapter.file_ext)).read_text() == "{}"
        finally:
            shutil.rmtree(directory)

    def test_directory_held_by_its_creator_is_not_removed(self, client: Client, array_collection: Collection):
        """The creator holds the lock of the collection, its json file, until it writes the file: the repair does not
        wait for it.
        """
        directory = client.root_path / "half-made"
        directory.mkdir()
        json_file = directory / ("half-made" + client._adapter.file_ext)
        try:
            finding = checker(client, IntegrityCheck.collections).check().findings[0]
            with Flock(json_file, create=True):
                with pytest.raises(DekerLockError) as error:
                    CollectionsCheck(client).repair(finding)
                assert str(error.value) == f"{json_file} is locked"
                report = checker(client, IntegrityCheck.collections, repair=True).check()
            assert kinds(report) == [(IntegrityCheck.collections, IntegrityFindingKind.collection_unreadable)]
            assert not report.findings[0].repaired
            assert report.findings[0].repair_error == f"{json_file} is locked"
            assert json_file.read_text() == ""
        finally:
            shutil.rmtree(directory)

    def test_stray_file_in_the_storage_root_is_ignored(self, client: Client, array_collection: Collection):
        stray = client.root_path / "notes.txt"
        stray.write_text("x")
        try:
            assert checker(client, IntegrityCheck.collections).check().ok
        finally:
            stray.unlink()

    def test_unknown_collection_raises(self, client: Client):
        with pytest.raises(DekerCollectionNotExistsError):
            checker(client).check("no_such_collection")


class TestMarkersCheck:
    """Markers of VArray writes left by Deker < 2 are reported and removed on repair."""

    def test_ok(self, client: Client, inserted_varray: VArray):
        inserted_varray[:].update(1.0)
        assert checker(client, IntegrityCheck.markers).check().ok

    def test_marker_is_reported_and_repaired(self, client: Client, inserted_varray: VArray, varray_collection):
        array = next(iter(varray_collection.arrays))
        file = main_file(array)
        marker = file.parent / f"{file.name}:1:{'0' * 32}{LocksExtensions.varray_lock.value}"
        marker.touch()
        try:
            report = checker(client, IntegrityCheck.markers).check(varray_collection.name)
            assert kinds(report) == [(IntegrityCheck.markers, IntegrityFindingKind.marker_left)]
            finding = report.findings[0]
            assert finding.collection == varray_collection.name
            assert finding.path == marker
            assert finding.repairable
            assert marker.exists()

            report = checker(client, IntegrityCheck.markers, repair=True).check(varray_collection.name)
            assert report.findings[0].repaired
            assert not marker.exists()
            assert file.exists()
        finally:
            marker.unlink(missing_ok=True)


class TestArraysCheck:
    """Every main file initialises an Array or a VArray from its metadata."""

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray", "inserted_array_with_attributes"])
    def test_ok(self, client: Client, request, fixture: str):
        array = request.getfixturevalue(fixture)
        assert checker(client, IntegrityCheck.arrays).check(array.collection).ok

    @pytest.mark.parametrize("fixture", ["inserted_array_with_attributes", "inserted_varray_with_attributes"])
    def test_broken_meta_is_reported(self, client: Client, request, fixture: str):
        """An unreadable file is reported once: the Arrays of an unreadable VArray are not blamed for it."""
        array = request.getfixturevalue(fixture)
        rewrite_meta(array, {"primary_attributes": {"incorrect_attribute": 10}})
        report = checker(client, IntegrityCheck.arrays).check(array.collection)
        assert kinds(report) == [(IntegrityCheck.arrays, IntegrityFindingKind.meta_unreadable)]
        finding = report.findings[0]
        assert finding.collection == array.collection
        assert finding.array_id == array.id
        assert finding.path == main_file(array)
        assert not finding.repairable
        assert array.id in finding.message

    def test_file_without_meta_is_reported(self, client: Client, inserted_array: Array):
        with h5py.File(main_file(inserted_array), "r+") as f:
            del f["meta"]
        report = checker(client, IntegrityCheck.arrays).check(inserted_array.collection)
        assert kinds(report) == [(IntegrityCheck.arrays, IntegrityFindingKind.meta_unreadable)]
        assert report.findings[0].array_id == inserted_array.id

    def test_arrays_of_a_varray_are_checked_too(self, client: Client, inserted_varray: VArray, varray_collection):
        array = next(iter(varray_collection.arrays))
        attributes = {"vid": inserted_varray.id, "v_position": list(array._v_position)}
        rewrite_meta(array, {"primary_attributes": {"incorrect_attribute": 10}})
        try:
            report = checker(client, IntegrityCheck.arrays).check(varray_collection.name)
            assert kinds(report) == [(IntegrityCheck.arrays, IntegrityFindingKind.meta_unreadable)]
            assert report.findings[0].array_id == array.id
        finally:
            rewrite_meta(array, {"primary_attributes": attributes})

    def test_unreadable_array_is_not_checked_by_the_array_checks(self, client: Client, inserted_array: Array):
        """A file whose metadata cannot be read is reported once: the checks that need the array skip it."""
        with h5py.File(main_file(inserted_array), "r+") as f:
            del f["meta"]
        report = checker(client, *IntegrityCheck).check(inserted_array.collection)
        assert kinds(report) == [(IntegrityCheck.arrays, IntegrityFindingKind.meta_unreadable)]

    def test_ok_for_an_array_of_a_varray(self, client: Client, timed_varray: VArray):
        assert checker(client, IntegrityCheck.arrays).check(timed_varray.collection).ok

    def test_an_array_without_a_varray_has_nothing_more_to_check(
        self, client: Client, inserted_array_with_attributes: Array
    ):
        assert checker(client, IntegrityCheck.arrays).check(inserted_array_with_attributes.collection).ok

    def test_array_of_a_missing_varray_is_reported(self, client: Client, inserted_varray: VArray, varray_collection):
        arrays = list(varray_collection.arrays)
        main_file(inserted_varray).unlink()
        symlink_of(inserted_varray).unlink()
        try:
            report = checker(client, IntegrityCheck.arrays).check(varray_collection.name)
            assert sorted(kinds(report)) == [(IntegrityCheck.arrays, IntegrityFindingKind.varray_missing)] * len(arrays)
            assert sorted(finding.array_id for finding in report.findings) == sorted(array.id for array in arrays)
            assert all(inserted_varray.id in finding.message for finding in report.findings)
            assert not any(finding.repairable for finding in report.findings)
        finally:
            for array in arrays:
                array.delete()

    def test_array_outside_the_grid_is_reported(self, client: Client, inserted_varray: VArray, varray_collection):
        array = next(iter(varray_collection.arrays))
        rewrite_meta(array, {"primary_attributes": {"vid": inserted_varray.id, "v_position": [0, 0, 5]}})
        report = checker(client, IntegrityCheck.arrays).check(varray_collection.name)
        assert kinds(report) == [(IntegrityCheck.arrays, IntegrityFindingKind.position_out_of_grid)]
        finding = report.findings[0]
        assert finding.array_id == array.id
        assert "v_position (0, 0, 5)" in finding.message and "vgrid (2, 2, 2)" in finding.message
        assert not finding.repairable

    def test_wrong_time_attribute_is_reported_and_repaired(self, client: Client, timed_varray: VArray):
        collection = timed_varray._VArray__collection
        array = collection.arrays.filter({"vid": timed_varray.id, "v_position": (0, 2)}).last()
        expected = array.custom_attributes["parent.time"]
        assert expected == datetime(2023, 1, 1, 8, tzinfo=timezone.utc)
        array.update_custom_attributes({"parent.time": expected - timedelta(hours=5)})
        report = checker(client, IntegrityCheck.arrays).check(collection.name)
        assert kinds(report) == [(IntegrityCheck.arrays, IntegrityFindingKind.time_attribute_wrong)]
        finding = report.findings[0]
        assert finding.array_id == array.id
        assert finding.path == main_file(array)
        assert "parent.time" in finding.message and expected.isoformat() in finding.message
        assert finding.repairable

        report = checker(client, IntegrityCheck.arrays, repair=True).check(collection.name)
        assert report.findings[0].repaired
        repaired = collection.arrays.filter({"id": array.id}).last()
        assert repaired.custom_attributes["parent.time"] == expected
        assert repaired.dimensions[1].start_value == expected
        assert checker(client, IntegrityCheck.arrays).check(collection.name).ok

    def test_every_array_is_checked(self, client: Client, timed_varray: VArray):
        """Each Array along the time axis has its own start: the drift of any of them is found."""
        collection = timed_varray._VArray__collection
        arrays = list(collection.arrays)
        for array in arrays:
            array.update_custom_attributes({"parent.time": datetime(2023, 1, 1, tzinfo=timezone.utc)})
        report = checker(client, IntegrityCheck.arrays).check(collection.name)
        assert sorted(finding.array_id for finding in report.findings) == sorted(
            array.id for array in arrays if array._v_position != (0, 0)
        )


class TestLinksCheck:
    """Main files and symlinks point at each other, one array per directory of primary attributes."""

    @pytest.mark.parametrize(
        "fixture",
        ["inserted_array", "inserted_array_with_attributes", "inserted_varray", "inserted_varray_with_attributes"],
    )
    def test_ok(self, client: Client, request, fixture: str):
        array = request.getfixturevalue(fixture)
        assert checker(client, IntegrityCheck.links).check(array.collection).ok

    @pytest.mark.parametrize(
        "fixture",
        ["inserted_array", "inserted_array_with_attributes", "inserted_varray", "inserted_varray_with_attributes"],
    )
    def test_missing_symlink_is_reported_and_repaired(self, client: Client, request, fixture: str):
        array = request.getfixturevalue(fixture)
        symlink = symlink_of(array)
        symlink.unlink()
        report = checker(client, IntegrityCheck.links).check(array.collection)
        assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.symlink_missing)]
        finding = report.findings[0]
        assert finding.collection == array.collection
        assert finding.array_id == array.id
        assert finding.path == symlink
        assert finding.repairable
        assert not symlink.exists()

        report = checker(client, IntegrityCheck.links, repair=True).check(array.collection)
        assert report.findings[0].repaired
        assert symlink.is_symlink()
        assert symlink.resolve() == main_file(array)
        assert checker(client, IntegrityCheck.links).check(array.collection).ok

    def test_missing_symlink_of_a_array_is_repaired(self, client: Client, inserted_varray: VArray, varray_collection):
        array = next(iter(varray_collection.arrays))
        symlink = symlink_of(array)
        symlink.unlink()
        report = checker(client, IntegrityCheck.links, repair=True).check(varray_collection.name)
        assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.symlink_missing)]
        assert report.findings[0].array_id == array.id
        assert symlink.resolve() == main_file(array)
        assert varray_collection.arrays.filter({"vid": inserted_varray.id, "v_position": array._v_position}).last()

    @pytest.mark.parametrize("fixture", ["inserted_array_with_attributes", "inserted_varray_with_attributes"])
    def test_broken_symlink_is_reported_and_repaired(self, client: Client, request, fixture: str):
        """A symlink whose file is gone is removed with the empty directories above it."""
        array = request.getfixturevalue(fixture)
        symlink = symlink_of(array)
        file = main_file(array)
        file.unlink()
        report = checker(client, IntegrityCheck.links).check(array.collection)
        assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.symlink_broken)]
        finding = report.findings[0]
        assert finding.collection == array.collection
        assert finding.array_id == array.id
        assert finding.path == symlink
        assert finding.repairable
        assert symlink.is_symlink()

        report = checker(client, IntegrityCheck.links, repair=True).check(array.collection)
        assert report.findings[0].repaired
        assert not symlink.is_symlink()
        assert not symlink.parent.exists()  # the directory of the attributes is empty and removed
        assert (array._adapter.collection_path / array._adapter.symlinks_dir).exists()
        assert checker(client, IntegrityCheck.links).check(array.collection).ok

    def test_misplaced_symlink_is_reported_and_repaired(self, client: Client, inserted_array_with_attributes: Array):
        """A symlink in the directory of other attributes is removed; the missing one is reported separately."""
        array = inserted_array_with_attributes
        symlink = symlink_of(array)
        wrong_dir = symlink.parent.parent / "999"
        wrong_dir.mkdir()
        misplaced = wrong_dir / symlink.name
        os.rename(symlink, misplaced)
        report = checker(client, IntegrityCheck.links).check(array.collection)
        assert sorted(kinds(report)) == sorted(
            [
                (IntegrityCheck.links, IntegrityFindingKind.symlink_missing),
                (IntegrityCheck.links, IntegrityFindingKind.symlink_misplaced),
            ]
        )
        by_kind = {finding.kind: finding for finding in report.findings}
        assert by_kind[IntegrityFindingKind.symlink_misplaced].path == misplaced
        assert by_kind[IntegrityFindingKind.symlink_misplaced].array_id == array.id
        assert by_kind[IntegrityFindingKind.symlink_misplaced].repairable
        assert by_kind[IntegrityFindingKind.symlink_missing].path == symlink

        report = checker(client, IntegrityCheck.links, repair=True).check(array.collection)
        assert all(finding.repaired for finding in report.findings)
        assert not misplaced.is_symlink()
        assert not wrong_dir.exists()
        assert symlink.resolve() == main_file(array)
        assert checker(client, IntegrityCheck.links).check(array.collection).ok

    def test_symlink_to_another_file_is_misplaced(
        self, client: Client, inserted_array_with_attributes: Array, array_collection_with_attributes: Collection
    ):
        """A symlink that does not point at the file of its name is a misplaced one."""
        array = inserted_array_with_attributes
        other = array_collection_with_attributes.create(
            {"primary_attribute": 99}, {"time_attr_name": datetime.now(timezone.utc)}
        )
        symlink = symlink_of(array)
        symlink.unlink()
        os.symlink(main_file(other), symlink)
        try:
            report = checker(client, IntegrityCheck.links).check(array.collection)
            assert sorted(kinds(report)) == sorted(
                [
                    (IntegrityCheck.links, IntegrityFindingKind.symlink_missing),
                    (IntegrityCheck.links, IntegrityFindingKind.symlink_misplaced),
                ]
            )
            assert all(finding.array_id == array.id for finding in report.findings)
        finally:
            other.delete()

    def test_two_arrays_in_one_directory_are_reported_and_not_repaired(
        self, client: Client, array_collection_with_attributes: Collection
    ):
        """Two arrays with the same primary attributes: the checker cannot tell which one is right."""
        collection = array_collection_with_attributes
        first = collection.create({"primary_attribute": 7}, {"time_attr_name": datetime.now(timezone.utc)})
        second = collection.create({"primary_attribute": 8}, {"time_attr_name": datetime.now(timezone.utc)})
        rewrite_meta(second, {"primary_attributes": {"primary_attribute": 7}})
        os.rename(symlink_of(second), symlink_of(first).parent / symlink_of(second).name)
        try:
            report = checker(client, IntegrityCheck.links).check(collection.name)
            assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.duplicate_arrays)]
            finding = report.findings[0]
            assert finding.path == symlink_of(first).parent
            assert first.id in finding.message and second.id in finding.message
            assert not finding.repairable

            before = files_of(collection.path)
            report = checker(client, IntegrityCheck.links, repair=True).check(collection.name)
            assert not report.findings[0].repaired
            assert files_of(collection.path) == before
        finally:
            for symlink in symlink_of(first).parent.iterdir():
                symlink.unlink()
            for array in (first, second):
                main_file(array).unlink()

    def test_two_arrays_for_one_position_are_reported(self, client: Client, inserted_varray: VArray, varray_collection):
        array = next(iter(varray_collection.arrays))
        twin = varray_collection.arrays.create(
            primary_attributes={"vid": inserted_varray.id, "v_position": (9, 9, 9)}, custom_attributes={}
        )
        rewrite_meta(twin, {"primary_attributes": {"vid": inserted_varray.id, "v_position": list(array._v_position)}})
        os.rename(symlink_of(twin), symlink_of(array).parent / symlink_of(twin).name)
        report = checker(client, IntegrityCheck.links).check(varray_collection.name)
        assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.duplicate_arrays)]
        assert report.findings[0].path == symlink_of(array).parent

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_array_with_attributes", "inserted_varray"])
    def test_stray_file_in_a_symlink_directory_is_reported(self, client: Client, request, fixture: str):
        """A plain file among the symlinks is taken for an array by the lookups: reported, not removed."""
        array = request.getfixturevalue(fixture)
        stray = symlink_of(array).parent / "notes.txt"
        stray.write_text("x")
        try:
            report = checker(client, IntegrityCheck.links).check(array.collection)
            assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.stray_file)]
            assert report.findings[0].path == stray
            assert not report.findings[0].repairable
            checker(client, IntegrityCheck.links, repair=True).check(array.collection)
            assert stray.exists()
        finally:
            stray.unlink()


TAIL = ".0123456789abcdef0123456789abcdef.tmp"


def temporary_of(array: Array | VArray) -> Path:
    """The temporary file an interrupted operation leaves next to the main file of an array.

    :param array: the array
    :returns: ``<id>.hdf5.<hex>.tmp`` of a full clearing of an Array, ``<id>.json.<hex>.tmp`` of an update of the
        custom attributes of a VArray
    """
    file = main_file(array)
    return file.with_name(file.name + TAIL)


class ReplaceOnFirstLock:
    """Stand for ``Flock.acquire``: the first lock taken, the main file at its path is replaced by a copy, as a live
    operation that finishes meanwhile does, or moved away, as a deletion does.

    :param real: the real ``Flock.acquire``
    :param main: the main file
    :param away: where the main file is moved; ``None`` - it is replaced
    """

    def __init__(self, real, main: Path, away: Path | None) -> None:
        self.real = real
        self.main = main
        self.away = away
        self.calls = 0

    def __call__(self, flock: Flock) -> None:
        self.calls += 1
        self.real(flock)
        if self.calls > 1:
            return
        if self.away is not None:
            os.rename(self.main, self.away)
            return
        copy = self.main.with_name("copy")
        shutil.copyfile(self.main, copy)
        os.replace(copy, self.main)


class TestTemporaryFiles:
    """A temporary file left in a data directory by an interrupted full clearing or update of the attributes of a
    VArray is a stray file; the repair removes it unless a live operation holds the main file."""

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray"])
    def test_temporary_file_is_reported_and_removed(self, client: Client, request, fixture: str):
        array = request.getfixturevalue(fixture)
        temporary = temporary_of(array)
        temporary.write_bytes(b"left")
        report = checker(client, IntegrityCheck.links).check(array.collection)
        assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.stray_file)]
        finding = report.findings[0]
        assert finding.path == temporary
        assert finding.collection == array.collection
        assert finding.array_id == array.id
        assert finding.repairable
        assert finding.message == f"Temporary file {temporary} is left by an interrupted operation"
        report = checker(client, IntegrityCheck.links, repair=True).check(array.collection)
        assert report.findings[0].repaired
        assert not temporary.exists()
        assert main_file(array).exists()
        assert checker(client, IntegrityCheck.links).check(array.collection).ok

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray"])
    @pytest.mark.parametrize("name", ["notes.txt", "{}.tmp", "{}.hdf5.tmp", "{}.json.tmp", "{}.json.tmp.bak"])
    def test_other_files_of_a_data_directory_are_not_temporary(self, client: Client, request, fixture: str, name: str):
        array = request.getfixturevalue(fixture)
        other = main_file(array).with_name(name.format(array.id))
        other.write_bytes(b"x")
        try:
            assert checker(client, IntegrityCheck.links).check(array.collection).ok
        finally:
            other.unlink()

    @pytest.mark.parametrize(
        ("fixture", "shared"),
        [pytest.param("inserted_array", True, id="full clearing"), pytest.param("inserted_varray", False, id="update")],
    )
    def test_temporary_file_of_a_live_operation_is_left(self, client: Client, request, fixture: str, shared: bool):
        """The full clearing holds the main file shared, the update of the attributes of a VArray holds its json file
        exclusively: the repair does not get the main file exclusively and leaves the temporary file."""
        array = request.getfixturevalue(fixture)
        temporary = temporary_of(array)
        temporary.write_bytes(b"live")
        try:
            with Flock(main_file(array), shared=shared):
                report = checker(client, IntegrityCheck.links, repair=True).check(array.collection)
            finding = report.findings[0]
            assert not finding.repaired
            assert finding.repair_error == f"{main_file(array)} is locked"
            assert temporary.exists()
        finally:
            temporary.unlink()

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray"])
    def test_temporary_file_gone_before_the_repair_is_not_an_error(self, client: Client, request, fixture: str):
        array = request.getfixturevalue(fixture)
        temporary = temporary_of(array)
        temporary.write_bytes(b"left")
        (finding,) = checker(client, IntegrityCheck.links).check(array.collection).findings
        temporary.unlink()  # its operation removed it
        assert LinksCheck(client).repair(finding) is None
        assert main_file(array).exists()

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray"])
    def test_main_file_replaced_while_it_is_locked_is_locked_again(self, client: Client, request, fixture: str, mocker):
        array = request.getfixturevalue(fixture)
        temporary = temporary_of(array)
        temporary.write_bytes(b"left")
        (finding,) = checker(client, IntegrityCheck.links).check(array.collection).findings
        stand = ReplaceOnFirstLock(Flock.acquire, main_file(array), None)
        mocker.patch.object(Flock, "acquire", autospec=True, side_effect=stand)
        assert LinksCheck(client).repair(finding) is None
        assert stand.calls == 2
        assert not temporary.exists()

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray"])
    def test_main_file_gone_while_it_is_locked_lets_the_temporary_file_go(
        self, client: Client, request, fixture: str, mocker, tmp_path: Path
    ):
        """The array is deleted once the lock is taken: its operation ends with its error, the file is removed."""
        array = request.getfixturevalue(fixture)
        temporary = temporary_of(array)
        temporary.write_bytes(b"left")
        (finding,) = checker(client, IntegrityCheck.links).check(array.collection).findings
        stand = ReplaceOnFirstLock(Flock.acquire, main_file(array), tmp_path / "deleted")
        mocker.patch.object(Flock, "acquire", autospec=True, side_effect=stand)
        assert LinksCheck(client).repair(finding) is None
        assert stand.calls == 1
        assert not temporary.exists()
        assert not main_file(array).parent.exists()  # emptied: removed up to the data directory
        main_file(array).parent.mkdir(parents=True)
        os.rename(tmp_path / "deleted", main_file(array))

    def test_temporary_file_without_its_main_file_gone_before_the_repair_is_not_an_error(
        self, client: Client, inserted_array: Array
    ):
        temporary = temporary_of(inserted_array)
        finding = IntegrityFinding(
            check=IntegrityCheck.links,
            kind=IntegrityFindingKind.stray_file,
            message="",
            collection=inserted_array.collection,
            path=temporary.with_name(
                "0" + temporary.name[1:] if temporary.name[0] != "0" else "1" + temporary.name[1:]
            ),
            repairable=True,
        )
        assert LinksCheck(client).repair(finding) is None

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray"])
    def test_temporary_file_without_its_main_file_is_removed_with_its_directories(
        self, client: Client, request, fixture: str
    ):
        """The array was deleted meanwhile: the operation that left the file ends with its error anyway."""
        array = request.getfixturevalue(fixture)
        main = main_file(array)
        data = array._adapter.collection_path / array._adapter.data_dir
        first = "0" if array.id[0] != "0" else "1"  # a tree of directories no array of the test shares
        other_id = first * 8 + "-" + array.id.split("-", 1)[1]
        directory = get_main_path(other_id, data)
        directory.mkdir(parents=True)
        temporary = directory / temporary_of(array).name.replace(array.id, other_id)
        temporary.write_bytes(b"left")
        report = checker(client, IntegrityCheck.links, repair=True).check(array.collection)
        assert kinds(report) == [(IntegrityCheck.links, IntegrityFindingKind.stray_file)]
        assert report.findings[0].array_id == other_id
        assert report.findings[0].repaired
        assert not (data / first).exists()
        assert main.exists()

    def test_stray_file_of_a_symlink_directory_is_refused_by_the_repair(self, client: Client, inserted_array: Array):
        """Only a temporary file of a data directory is removed: a plain file among the symlinks is not."""
        stray = symlink_of(inserted_array).parent / "notes.txt"
        finding = IntegrityFinding(
            check=IntegrityCheck.links,
            kind=IntegrityFindingKind.stray_file,
            message="",
            collection=inserted_array.collection,
            path=stray,
            repairable=True,
        )
        with pytest.raises(DekerIntegrityError) as error:
            LinksCheck(client).repair(finding)
        assert str(error.value) == "stray_file cannot be repaired"


class TestDataCheck:
    """One cell of every array reads back with the dtype of the collection."""

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray", "array"])
    def test_ok(self, client: Client, request, fixture: str):
        array = request.getfixturevalue(fixture)
        if fixture == "array":
            array._adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert checker(client, IntegrityCheck.data).check(array.collection).ok

    @pytest.mark.parametrize(
        ("dtype", "shape", "kind"),
        [
            (int, (10, 10, 10), IntegrityFindingKind.dtype_mismatch),
            (np.float64, (1, 1, 1), IntegrityFindingKind.data_unreadable),
        ],
    )
    def test_broken_data_is_reported(
        self, client: Client, inserted_array: Array, dtype: type, shape: tuple[int, ...], kind: IntegrityFindingKind
    ):
        with h5py.File(main_file(inserted_array), "r+") as f:
            del f["data"]
            f.create_dataset("data", dtype=dtype, shape=shape)
        report = checker(client, IntegrityCheck.data).check(inserted_array.collection)
        assert kinds(report) == [(IntegrityCheck.data, kind)]
        finding = report.findings[0]
        assert finding.array_id == inserted_array.id
        assert finding.path == main_file(inserted_array)
        assert not finding.repairable

    @pytest.mark.parametrize(
        ("error", "critical"),
        [
            pytest.param(DekerCriticalError("Storage directory /nowhere doesn't exist"), True, id="critical"),
            pytest.param(OSError("unreadable"), False, id="other"),
        ],
    )
    def test_critical_error_of_the_read_is_raised_not_reported(
        self, client: Client, inserted_array: Array, mocker, error: Exception, critical: bool
    ):
        mocker.patch.object(inserted_array._adapter, "read_data", side_effect=error)
        target = SimpleNamespace(
            array=inserted_array,
            files=SimpleNamespace(collection=inserted_array._Array__collection),
            main_file=main_file(inserted_array),
        )
        if critical:
            with pytest.raises(DekerCriticalError) as info:
                DataCheck(client).run(target)
            assert info.value is error
        else:
            (finding,) = DataCheck(client).run(target)
            assert finding.kind == IntegrityFindingKind.data_unreadable
            assert finding.message == f"Array {inserted_array.id} data is corrupted: unreadable"

    def test_data_is_not_read_by_the_default_checks(self, client: Client, inserted_array: Array):
        with h5py.File(main_file(inserted_array), "r+") as f:
            del f["data"]
            f.create_dataset("data", dtype=np.float64, shape=(1, 1, 1))
        assert checker(client).check(inserted_array.collection).ok


class TestCountersCheck:
    """The counter of empty cells of every array equals the number of fill values in its data."""

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray", "array"])
    def test_ok(self, client: Client, request, fixture: str):
        array = request.getfixturevalue(fixture)
        if fixture == "array":
            array._adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert checker(client, IntegrityCheck.counters).check(array.collection).ok

    @pytest.mark.parametrize("stored", [0, 5, 1000])
    def test_drifted_counter_is_reported_and_repaired(self, client: Client, inserted_array: Array, stored: int):
        inserted_array[0].update(np.nan)  # 100 cells of 1000 are empty now
        with h5py.File(main_file(inserted_array), "r+") as f:
            f["empty_cells"][()] = stored
        report = checker(client, IntegrityCheck.counters).check(inserted_array.collection)
        assert kinds(report) == [(IntegrityCheck.counters, IntegrityFindingKind.counter_drifted)]
        finding = report.findings[0]
        assert finding.array_id == inserted_array.id
        assert finding.path == main_file(inserted_array)
        assert str(stored) in finding.message and "100" in finding.message
        assert finding.repairable

        report = checker(client, IntegrityCheck.counters, repair=True).check(inserted_array.collection)
        assert report.findings[0].repaired
        with h5py.File(main_file(inserted_array), "r") as f:
            assert int(f["empty_cells"][()]) == 100
        assert checker(client, IntegrityCheck.counters).check(inserted_array.collection).ok

    def test_full_clearing_leaves_a_true_counter(self, client: Client, inserted_array: Array):
        inserted_array[:].clear()
        assert checker(client, IntegrityCheck.counters).check(inserted_array.collection).ok
        inserted_array._adapter.empty_trash(background=False)

    def test_counters_are_not_read_by_the_default_checks(self, client: Client, inserted_array: Array):
        with h5py.File(main_file(inserted_array), "r+") as f:
            f["empty_cells"][()] = 5
        assert checker(client).check(inserted_array.collection).ok
        assert checker(client, *IntegrityCheck).check(inserted_array.collection).findings


class TestIntegrityChecker:
    """The walker: what is checked, in which order, with how many threads, and when it stops."""

    @pytest.fixture
    def broken_collections(self, client: Client, array_schema) -> list[Collection]:
        """Two collections, each with one array without a symlink."""
        collections = [client.create_collection(f"broken_{n}", array_schema) for n in range(2)]
        for collection in collections:
            symlink_of(collection.create()).unlink()
        yield collections
        for collection in collections:
            collection.delete()

    def test_only_the_named_collection_is_checked(self, client: Client, broken_collections: list[Collection]):
        report = checker(client).check(broken_collections[0].name)
        assert [finding.collection for finding in report.findings] == [broken_collections[0].name]

    def test_every_collection_is_checked_without_a_name(self, client: Client, broken_collections: list[Collection]):
        report = checker(client).check()
        assert sorted(finding.collection for finding in report.findings) == sorted(c.name for c in broken_collections)

    def test_only_the_selected_checks_run(self, client: Client, broken_collections: list[Collection]):
        assert checker(client, IntegrityCheck.collections, IntegrityCheck.arrays).check().ok

    def test_stop_on_error_raises_at_the_first_finding(self, client: Client, broken_collections: list[Collection]):
        with pytest.raises(DekerIntegrityError) as info:
            checker(client, stop_on_error=True).check()
        assert "Symlink" in str(info.value)
        assert str(info.value).count("\t- ") == 1

    def test_stop_on_error_passes_a_clean_storage(self, client: Client, inserted_array: Array):
        assert checker(client, stop_on_error=True).check().ok

    @pytest.mark.parametrize("threads", [1, 2, 4])
    def test_workers_find_the_same(self, client: Client, broken_collections: list[Collection], threads: int):
        report = checker(client, workers=threads).check()
        assert sorted(kinds(report)) == [(IntegrityCheck.links, IntegrityFindingKind.symlink_missing)] * 2

    @pytest.mark.parametrize("threads", [1, 3])
    def test_workers_check_every_array(self, client: Client, varray_collection: Collection, threads: int):
        varray = varray_collection.create()
        varray[:].update(1.0)
        arrays = list(varray_collection.arrays)
        for array in arrays:
            symlink_of(array).unlink()
        try:
            report = checker(client, workers=threads).check(varray_collection.name)
            assert sorted(finding.array_id for finding in report.findings) == sorted(array.id for array in arrays)
        finally:
            for array in arrays:
                array.delete()
            varray.delete()

    def test_check_without_repair_touches_nothing(self, client: Client, broken_collections: list[Collection]):
        before = files_of(client.root_path)
        checker(client, *IntegrityCheck).check()
        assert files_of(client.root_path) == before

    def test_repair_fixes_what_it_can_and_reports_the_rest(self, client: Client, broken_collections: list[Collection]):
        collection = broken_collections[0]
        array = collection.create()
        with h5py.File(main_file(array), "r+") as f:
            del f["meta"]
        report = checker(client, repair=True).check(collection.name)
        assert sorted(kinds(report)) == sorted(
            [
                (IntegrityCheck.arrays, IntegrityFindingKind.meta_unreadable),
                (IntegrityCheck.links, IntegrityFindingKind.symlink_missing),
            ]
        )
        assert [finding.kind for finding in report.unrepaired] == [IntegrityFindingKind.meta_unreadable]
        assert checker(client, IntegrityCheck.collections, IntegrityCheck.links).check(collection.name).ok

    def test_repair_failure_is_reported_not_raised(self, client: Client, broken_collections: list[Collection], mocker):
        mocker.patch.object(LinksCheck, "repair", side_effect=OSError("Read-only file system"))
        report = checker(client, repair=True).check(broken_collections[0].name)
        assert len(report.findings) == 1
        finding = report.findings[0]
        assert not finding.repaired
        assert finding.repair_error == "Read-only file system"
        assert report.unrepaired == [finding]

    @pytest.mark.parametrize(
        ("error", "critical"),
        [
            pytest.param(DekerCriticalError("Storage directory /nowhere doesn't exist"), True, id="critical"),
            pytest.param(OSError("Read-only file system"), False, id="other"),
        ],
    )
    def test_critical_error_of_a_repair_is_raised_not_reported(
        self, client: Client, broken_collections: list[Collection], mocker, error: Exception, critical: bool
    ):
        mocker.patch.object(LinksCheck, "repair", side_effect=error)
        if critical:
            with pytest.raises(DekerCriticalError) as info:
                checker(client, repair=True).check(broken_collections[0].name)
            assert info.value is error
        else:
            (finding,) = checker(client, repair=True).check(broken_collections[0].name).findings
            assert finding.repair_error == "Read-only file system"

    @pytest.mark.parametrize(
        ("error", "critical"),
        [
            pytest.param(DekerCriticalError("Storage directory /nowhere doesn't exist"), True, id="critical"),
            pytest.param(ValueError("broken metadata"), False, id="other"),
        ],
    )
    def test_critical_error_of_the_initialisation_of_an_array_is_raised_not_reported(
        self, client: Client, inserted_array: Array, mocker, error: Exception, critical: bool
    ):
        collection = inserted_array._Array__collection
        mocker.patch.object(type(collection.arrays), "filter", side_effect=error)
        entry = SimpleNamespace(
            files=SimpleNamespace(collection=collection),
            is_varray=False,
            array_id=inserted_array.id,
            array=None,
            error="",
        )
        if critical:
            with pytest.raises(DekerCriticalError) as info:
                checker(client)._read_array(entry)
            assert info.value is error
        else:
            assert checker(client)._read_array(entry) is entry
            assert entry.error == "broken metadata"

    def test_not_repairable_strategy_refuses(self, client: Client):
        finding = IntegrityFinding(check=IntegrityCheck.data, kind=IntegrityFindingKind.data_unreadable, message="m")
        with pytest.raises(DekerIntegrityError, match="cannot be repaired"):
            DataCheck(client).repair(finding)

    @pytest.mark.parametrize("strategy", list(INTEGRITY_CHECKS.values()))
    def test_strategy_refuses_a_finding_of_another_kind(self, client: Client, strategy: type[IntegrityCheckStrategy]):
        """A finding of a kind the strategy does not repair is refused, whatever its fields say."""
        finding = IntegrityFinding(
            check=strategy.check,
            kind=IntegrityFindingKind.stray_file,
            message="m",
            collection="no_such_collection",
            array_id="a",
            path=Path("/no/such/file"),
        )
        with pytest.raises(DekerIntegrityError, match="cannot be repaired"):
            strategy(client).repair(finding)

    def test_progress_is_logged(self, client: Client, inserted_array: Array, deker_log: pytest.LogCaptureFixture):
        checker(client).check()
        assert any("Integrity check" in message for message in deker_log.messages)
