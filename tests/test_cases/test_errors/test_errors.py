"""Tests of the errors module."""

import copy
import pickle

import pytest

from cantok import SimpleToken, TimeoutToken
from deker_tools.cancel import TokenCancelledError, TokenTimeoutError

from deker.errors import (
    DekerBaseApplicationError,
    DekerCancelledError,
    DekerLockError,
    DekerTimeoutError,
    DekerValidationError,
    DekerVSubsetError,
    TokenErrorConverter,
)


class TestDekerVSubsetError:
    """The error of a virtual subset lists the exceptions of its threads, numbered from one."""

    def test_str_enumerates_the_exceptions(self):
        error = DekerVSubsetError("Data MAY BE NOW CORRUPTED", ["ValueError: Array 1", "OSError: Array 2"])
        assert str(error) == "Data MAY BE NOW CORRUPTED; exceptions:\n\n1) ValueError: Array 1\n2) OSError: Array 2 "
        assert error.message == "Data MAY BE NOW CORRUPTED"
        assert error.exceptions == ["ValueError: Array 1", "OSError: Array 2"]

    def test_str_without_exceptions(self):
        error = DekerVSubsetError("nothing happened", [])
        assert str(error) == "nothing happened; exceptions:\n\n "

    def test_is_an_application_error(self):
        assert issubclass(DekerVSubsetError, DekerBaseApplicationError)


class TestDekerValidationError:
    """The validation error names the dictionaries of the arguments it is in; its text and its args are as before."""

    @pytest.mark.parametrize(
        ("keywords", "parameters"),
        [
            pytest.param({}, (), id="not-given"),
            pytest.param({"parameters": ("custom_attributes",)}, ("custom_attributes",), id="one"),
            pytest.param(
                {"parameters": ("primary_attributes", "custom_attributes")},
                ("primary_attributes", "custom_attributes"),
                id="both",
            ),
        ],
    )
    def test_parameters_survive_pickle_and_copy(self, keywords: dict, parameters: tuple):
        """The errors cross the processes pickled: the text and the parameters come back."""
        error = DekerValidationError("bad", **keywords)
        assert error.parameters == parameters
        assert str(error) == "bad"
        assert error.args == ("bad",)
        assert repr(error) == "DekerValidationError('bad')"
        for twin in (pickle.loads(pickle.dumps(error)), copy.copy(error), copy.deepcopy(error)):
            assert type(twin) is DekerValidationError
            assert str(twin) == "bad"
            assert twin.args == ("bad",)
            assert twin.parameters == parameters


class TestCancellationErrors:
    """A cancelled operation raises DekerCancelledError; a read over its timeout raises its subclass."""

    def test_cancel_error_is_an_application_error(self):
        assert issubclass(DekerCancelledError, DekerBaseApplicationError)

    def test_timeout_error_is_a_cancel_error(self):
        assert issubclass(DekerTimeoutError, DekerCancelledError)


class TestTokenErrorConverter:
    """The errors of the tokens of deker-tools become the errors of DEKER, without a chain of tracebacks."""

    @pytest.mark.parametrize(
        ("raised", "expected"),
        [
            pytest.param(TokenTimeoutError("time is over", TimeoutToken(1)), DekerTimeoutError, id="timeout"),
            pytest.param(TokenCancelledError("cancelled", SimpleToken()), DekerCancelledError, id="cancelled"),
        ],
    )
    def test_token_errors_become_deker_errors(self, raised, expected):
        with pytest.raises(expected) as error, TokenErrorConverter():
            raise raised
        assert type(error.value) is expected
        assert str(error.value) == str(raised)
        assert error.value.__cause__ is None
        assert error.value.__suppress_context__ is True

    @pytest.mark.parametrize(
        "raised",
        [
            pytest.param(DekerLockError("held"), id="deker-error"),
            pytest.param(ValueError("value"), id="other-error"),
        ],
    )
    def test_other_errors_pass_as_they_are(self, raised):
        with pytest.raises(type(raised)) as error, TokenErrorConverter():
            raise raised
        assert error.value is raised

    def test_nothing_happens_without_an_error(self):
        with TokenErrorConverter():
            result = 1
        assert result == 1
