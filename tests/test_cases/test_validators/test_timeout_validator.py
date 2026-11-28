import pytest

from deker_tools.typings import Sentinel

from deker.errors import DekerValidationError
from deker.validators import validate_timeout


@pytest.mark.parametrize("timeout", [1, 60, 300, 100_000])
def test_positive_integer_is_accepted(timeout):
    validate_timeout(timeout=timeout)


@pytest.mark.parametrize(
    "timeout",
    [
        pytest.param(0, id="zero"),
        pytest.param(-1, id="negative"),
        pytest.param(1.5, id="float"),
        pytest.param(True, id="bool"),
        pytest.param("10", id="string"),
        pytest.param(None, id="none"),
    ],
)
def test_anything_else_is_rejected(timeout):
    with pytest.raises(DekerValidationError, match="positive integer number of seconds"):
        validate_timeout(timeout=timeout)


def test_timeout_not_given_is_accepted():
    validate_timeout(timeout=Sentinel)
