"""Tests of the private types: paths of an array."""

from pathlib import Path

import pytest

from deker.types.private.classes import Paths


class TestPathsCreate:
    def test_directories_are_created(self, tmp_path: Path):
        paths = Paths(main=tmp_path / "data" / "a" / "b", symlink=tmp_path / "symlinks" / "c")
        paths.create()
        assert paths.main.is_dir()
        assert paths.symlink.is_dir()

    @pytest.mark.parametrize("extension", ["hdf5", "json"])
    def test_file_paths_create_their_directories_only(self, tmp_path: Path, extension: str):
        """A path of a data file (hdf5 or json) names the file to be: its directory is created, the file is not."""
        main = tmp_path / "data" / "a" / f"array.{extension}"
        symlink = tmp_path / "symlinks" / "b" / f"array.{extension}"
        Paths(main=main, symlink=symlink).create()
        assert main.parent.is_dir()
        assert symlink.parent.is_dir()
        assert not main.exists()
        assert not symlink.exists()

    def test_existing_directories_are_kept(self, tmp_path: Path):
        existing = tmp_path / "data"
        existing.mkdir()
        marker = existing / "keep.txt"
        marker.write_text("keep")
        Paths(main=existing, symlink=tmp_path / "symlinks").create()
        assert marker.read_text() == "keep"
