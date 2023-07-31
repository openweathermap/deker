import pytest

from deker.dimensions import Dimension
from deker.errors import DekerValidationError
from deker.types import IndexLabels


class TestIndexLabels:
    ok_params = (
        [
            ["1", "2"],
            ("1", "2"),
            [0.1, 0.2],
            (0.1, 0.2),
        ],
    )
    invalid_params = [
        (
            [("1", 2), ("2", 3)],
            [["1", 2], ["2", 3]],
            (("1", 2), ("3", 4)),
            (["1", 2], ["3", 4]),
            {"1": 2, "3": 4},
            [1, 2],
            (1, 2),
        )
    ]

    @pytest.mark.parametrize("labels", *ok_params)
    def test_labels_ok(self, labels):
        """Test labels ok."""
        d = Dimension(name="name", size=2, labels=labels)
        labs = IndexLabels(labels)
        assert isinstance(d.labels, IndexLabels)
        assert d.labels == labs
        assert d.labels.first == labs.first
        assert d.labels.last == labs.last

    @pytest.mark.parametrize(
        "invalid_labels",
        [
            None,
            *invalid_params[0],
        ],
    )
    def test_labels_raises(self, invalid_labels):
        """Test labels validation."""
        with pytest.raises(DekerValidationError):
            IndexLabels(invalid_labels)

    @pytest.mark.parametrize("invalid_labels", *invalid_params)
    def test_labels_raises_from_dim(self, invalid_labels):
        """Test labels validation raises in incorrect dimension."""
        with pytest.raises(DekerValidationError):
            Dimension(name="name", size=2, labels=invalid_labels)

    def test_labels_None(self):
        d = Dimension(name="name", size=2, labels=None)
        assert d.labels is None

    @pytest.mark.parametrize("labels", *ok_params)
    def test_labels_str(self, labels):
        labs = IndexLabels(labels)
        assert str(labs) == f"{str(tuple(labels))}"

    @pytest.mark.parametrize("labels", *ok_params)
    def test_labels_repr(self, labels):
        labs = IndexLabels(labels)
        assert repr(labs) == f"IndexLabels({str(tuple(labels))})"


if __name__ == "__main__":
    pytest.main()
