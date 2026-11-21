import pytest

from deker.ctx import CTX


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        pytest.param({}, {}, id="not-given"),
        pytest.param({"extra": {"httpx_conf": {"timeout": 1}}}, {"httpx_conf": {"timeout": 1}}, id="given"),
    ],
)
def test_extra_of_the_context(uri, config, kwargs, expected):
    assert CTX(uri, config, **kwargs).extra == expected


def test_extra_not_given_is_a_new_dict_every_time(uri, config):
    first, second = CTX(uri, config), CTX(uri, config)
    first.extra["key"] = "value"
    assert second.extra == {}
