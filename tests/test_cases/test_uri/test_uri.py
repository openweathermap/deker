import sys

from pathlib import Path

import pytest

from deker.errors import DekerValidationError
from deker.uri import Uri


@pytest.mark.parametrize(
    (
        "uri",
        "scheme",
        "username",
        "password",
        "hostname",
        "port",
        "netloc",
        "path",
        "params",
        "query",
        "fragment",
    ),
    [
        pytest.param(
            "file:///var/tmp/collections",
            "file",
            None,
            None,
            None,
            None,
            "",
            "/var/tmp/collections",
            "",
            {},
            "",
            marks=pytest.mark.xfail(sys.platform == "darwin", reason="No /var/tmp on MacOS"),
        ),
        (
            "http://user:pass@host:8080/data/collections/",
            "http",
            "user",
            "pass",
            "host",
            8080,
            "user:pass@host:8080",
            "/data/collections/",
            "",
            {},
            "",
        ),
        (
            "http://user:pass@host:8080/data/collections/?collectionStorage="
            "MongodbStorageAdapter&dataStorage=TiffStorageAdapter",
            "http",
            "user",
            "pass",
            "host",
            8080,
            "user:pass@host:8080",
            "/data/collections/",
            "",
            {"collectionStorage": ["MongodbStorageAdapter"], "dataStorage": ["TiffStorageAdapter"]},
            "",
        ),
    ],
)
def test_create_uri(
    uri, scheme, username, password, hostname, port, netloc, path, params, query, fragment
):
    """Tests parsing Uri."""
    result = Uri.create(uri)
    assert result.scheme == scheme
    assert result.username == username
    assert result.password == password
    assert result.hostname == hostname
    assert result.port == port
    assert result.netloc == netloc
    assert result.path == path
    assert result.params == params
    assert result.query == query
    assert result.fragment == fragment


@pytest.mark.xfail()
@pytest.mark.parametrize(
    "uri_str",
    [
        pytest.param(
            "file:///var/tmp/collections",
            marks=pytest.mark.xfail(sys.platform == "darwin", reason="No /var/tmp on MacOS"),
        ),
        "http://user:pass@host:8080/data/collections/",
        "http://user:pass@host:8080/data/collections/?collectionStorage=4",
    ],
)
def test_uri_raw(uri_str):
    """Tests Uri raw property."""
    uri = Uri.create(uri_str)
    uri_url = uri.scheme + "://"
    if uri.netloc:
        uri_url += uri.netloc
    uri_url += str(uri.path)
    assert uri.raw_url == uri_url


@pytest.mark.parametrize(
    "uri",
    [
        "",
        " ",
        "       ",
        "\n",
        "\t",
        "\r",
        None,
        0,
        [],
        {},
    ],
)
def test_uri_raises_invalid_uri(uri):
    with pytest.raises(DekerValidationError):
        Uri.create(uri)


@pytest.mark.parametrize(
    "string",
    [
        "file:///tmp/data/collections",
        "http://host:8080/data/collections/",
        "https://user:pass@host:8080/data/collections/",
        "http://host:8080/data/collections/",
        "https://user:pass@host:8080/data/collections/",
    ],
)
def test_uri_path_concatenation(string):
    """Test if Uri joins to another string / path correctly."""
    uri = Uri.create(string)
    new_uri = uri / Path("some_path") / "some_extra" / "/some_slash_extra"
    assert new_uri.raw_url == "/".join(
        (uri.raw_url, "some_path", "some_extra", "/some_slash_extra")
    )


@pytest.mark.parametrize(
    "string",
    [
        "file:///tmp/data/collections",
        "http://host:8080/data/collections/",
        "https://user:pass@host:8080/data/collections/",
        "http://host:8080/data/collections/",
        "https://user:pass@host:8080/data/collections/",
    ],
)
def test_uri_path_concatenation_with_assignment_wrong_expectations(string):
    init_uri = uri = Uri.create(string)
    uri /= Path("some_path") / "some_extra" / "/some_slash_extra"
    assert uri.raw_url != "/".join(
        (init_uri.raw_url, "some_path", "some_extra", "/some_slash_extra")
    )
    assert uri.raw_url == "/".join((init_uri.raw_url, "/some_slash_extra"))


@pytest.mark.parametrize(
    "string",
    [
        "file:///tmp/data/collections",
        "http://host:8080/data/collections/",
        "https://user:pass@host:8080/data/collections/",
        "http://host:8080/data/collections/",
        "https://user:pass@host:8080/data/collections/",
    ],
)
def test_uri_path_correct_concatenation_with_assignment(string):
    init_uri = uri = Uri.create(string)
    uri /= Path("some_path") / "some_extra"
    assert uri.raw_url == "/".join((init_uri.raw_url, "some_path", "some_extra"))


if __name__ == "__main__":
    pytest.main()
