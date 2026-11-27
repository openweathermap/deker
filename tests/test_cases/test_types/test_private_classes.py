"""Tests of the private types: paths of an array."""

import os

from pathlib import Path

import pytest

from deker.errors import DekerInstanceNotExistsError
from deker.types.private.classes import Paths


class RemovedOnceMade:
    """Stand for ``os.mkdir``: the first directory made is removed at once, as a clearing of the collection does.

    :param original: the real ``os.mkdir``
    """

    def __init__(self, original) -> None:
        self.original = original
        self.calls = 0

    def __call__(self, path, *args, **kwargs) -> None:
        self.original(path, *args, **kwargs)
        self.calls += 1
        if self.calls == 1:
            os.rmdir(path)


class TestPathsCreate:
    """The directories of an array are made inside its collection, level by level; the collection is never made."""

    def test_directories_are_created_level_by_level(self, tmp_path: Path, mocker):
        collection = tmp_path / "C"
        collection.mkdir()
        paths = Paths(main=collection / "data" / "a" / "b", symlink=collection / "symlinks" / "c")
        mkdir = mocker.spy(os, "mkdir")
        paths.create(collection)
        assert paths.main.is_dir()
        assert paths.symlink.is_dir()
        made = ["data", "data/a", "data/a/b", "symlinks", "symlinks/c"]
        assert [call.args[0] for call in mkdir.call_args_list] == [str(collection / name) for name in made]

    def test_missing_collection_is_not_made(self, tmp_path: Path):
        collection = tmp_path / "C"
        paths = Paths(main=collection / "data" / "a", symlink=collection / "symlinks")
        with pytest.raises(DekerInstanceNotExistsError) as error:
            paths.create(collection)
        assert str(error.value) == f"{collection} doesn't exist"
        assert not collection.exists()

    def test_level_gone_meanwhile_is_not_made_again(self, tmp_path: Path, mocker):
        collection = tmp_path / "C"
        collection.mkdir()
        paths = Paths(main=collection / "data" / "a", symlink=collection / "symlinks")
        mocker.patch.object(os, "mkdir", side_effect=RemovedOnceMade(os.mkdir))
        with pytest.raises(DekerInstanceNotExistsError) as error:
            paths.create(collection)
        assert str(error.value) == f"{collection / 'data'} doesn't exist"
        assert os.listdir(collection) == []

    def test_existing_directories_are_kept(self, tmp_path: Path):
        collection = tmp_path / "C"
        existing = collection / "data"
        existing.mkdir(parents=True)
        marker = existing / "keep.txt"
        marker.write_text("keep")
        Paths(main=existing / "a", symlink=collection / "symlinks").create(collection)
        assert marker.read_text() == "keep"
        assert (existing / "a").is_dir()
        assert (collection / "symlinks").is_dir()
