from typing import Any

import pytest

from deker.arrays import Array
from deker.dimensions import Dimension
from deker.errors import DekerValidationError
from deker.types import IndexLabels


class TestDimensionNameValidation:
    @pytest.mark.parametrize(
        "name",
        [
            "1",
            "ok",
            "spaces in",
            " spaces in",
            " spaces in ",
            "spaces in ",
            " spacesin",
            " spacesin ",
            "spacesin ",
        ],
    )
    def test_dimension_name_ok(self, name: str):
        """Test dimension validates correct names."""
        d = Dimension(name=name, size=1)
        assert d
        assert d.name == name

    @pytest.mark.parametrize(
        "name",
        [
            None,
            True,
            False,
            0,
            1,
            -1,
            0.1,
            -0.1,
            tuple(),
            tuple("abc"),
            ("abc", "def"),
            set(),
            {"abc"},
            {("abc", "def")},
            dict(),
            {"abc": 1},
            {"abc": "def"},
            [],
            ["abc"],
            [("abc", "def")],
        ],
    )
    def test_dimension_name_raises_type_error(self, name: Any):
        """Test invalid names raise exceptions."""
        with pytest.raises(DekerValidationError):
            assert Dimension(name=name, size=1)

    @pytest.mark.parametrize(
        "name",
        [
            "",
            " ",
            "          ",
        ],
    )
    def test_dimension_empty_name_raises_value_error(self, name: str):
        """Test empty names raise exceptions."""
        with pytest.raises(DekerValidationError):
            assert Dimension(name=name, size=1)


class TestDimensionSizeValidators:
    @pytest.mark.parametrize(
        "size", [1, 2, 200, 100000, 9223372036854775807]  # maximal valid value: (2**63 - 1)
    )
    def test_dimension_size_ok(self, size: int):
        """Test size ok."""
        d = Dimension(name="name", size=size)
        assert d
        assert d.size == size

    @pytest.mark.parametrize(
        "size",
        [
            None,
            True,
            False,
            "",
            " ",
            "        ",
            "0",
            "1",
            "-1",
            0.1,
            -0.1,
            tuple(),
            tuple("abc"),
            ("abc", "def"),
            set(),
            {"abc"},
            {("abc", "def")},
            dict(),
            {"abc": 1},
            {"abc": "def"},
            [],
            ["abc"],
            [("abc", "def")],
        ],
    )
    def test_dimension_size_raises_type_error(self, size: Any):
        """Test invalid size types raise exceptions."""
        with pytest.raises(DekerValidationError):
            assert Dimension(name="name", size=size)

    @pytest.mark.parametrize(
        "size",
        [
            0,
            -1,
            -10000000,
        ],
    )
    def test_dimension_size_raises_value_error(self, size: Any):
        """Test invalid size values raise exceptions."""
        with pytest.raises(DekerValidationError):
            assert Dimension(name="name", size=size)


class TestDimensionLabelsValidation:
    @pytest.mark.parametrize(
        "labels",
        [
            None,
            ["1", "2"],
            [0.2, 0.1],
            ("1", "2"),
            (0.2, 0.1),
        ],
    )
    def test_dimension_labels_ok(self, labels):
        """Test labels initialization."""
        d = Dimension(name="name", size=2, labels=labels)
        assert d
        if labels is None:
            assert d.labels is None
        else:
            labs = IndexLabels(labels)
            assert isinstance(d.labels, IndexLabels)
            assert d.labels == labs
            assert d.labels.first == labs.first
            assert d.labels.last == labs.last

    @pytest.mark.parametrize(
        "labels",
        [
            True,
            False,
            "",
            " ",
            "        ",
            "0",
            "1",
            "-1",
            1,
            2,
            3,
            0.1,
            -0.1,
            set(),
            {"abc"},
            {("abc", "def")},
            [1, 2],
            (1, 2),
            [],
            [(1, 2), (3, 4)],
            tuple(),
            ((1, 2), (3, 4)),
            dict(),
            {1: 2, 3: 4},
            [("1", 2), ("2", 3)],
            [["1", 2], ["2", 3]],
            (("1", 2), ("3", 4)),
            (["1", 2], ["3", 4]),
            ["1", 2],
            [1, "2"],
            [0.2, 1],
            [0.2, "1"],
            ("1", 2),
            (1, "2"),
            (0.2, 1),
            (0.2, "1"),
            {"1": 2, "3": 4},
        ],
    )
    def test_dimension_labels_raises_type_error(self, labels: Any):
        """Test invalid labels types raise exceptions."""
        with pytest.raises(DekerValidationError):
            assert Dimension(name="name", size=2, labels=labels)

    @pytest.mark.parametrize(
        "labels",
        [
            ["1"],
            [1, 2, 3],
            [0.3, 0.4],
        ],
    )
    def test_dimension_labels_invalid_size_raise(self, labels):
        """Test labels wrong values quantity."""
        with pytest.raises(DekerValidationError):
            assert Dimension(name="name", size=4, labels=labels)

    def test_dimension_repr(self, array: Array):
        for dim in array.dimensions:
            s = (
                f"{dim.__class__.__name__}("
                f"name={dim.name!r}, "
                f"size={dim.size!r}, "
                f"step={dim.step!r}"
                f")"
            )
            if hasattr(dim, "labels") and dim.labels is not None:
                s = s[:-1] + f", labels={dim.labels})"
            if hasattr(dim, "scale") and dim.scale is not None:
                s = s[:-1] + f", scale={dim.scale})"
            if hasattr(dim, "start_value"):
                s = s[:-1] + f", start_value={dim.start_value!r})"

            assert repr(dim) == s

    def test_dimension_str(self, array: Array):
        for dim in array.dimensions:
            assert str(dim) == str(dim.size)


if __name__ == "__main__":
    pytest.main()
