"""Tests of the errors module."""

from deker.errors import DekerBaseApplicationError, DekerVSubsetError


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
