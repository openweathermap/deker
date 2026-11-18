import pytest

from attr import fields

from deker.config import DekerConfig

uri: str
workers: int
write_lock_timeout: int
write_lock_check_interval: int
memory_limit: int
loglevel: str = "DEBUG"

# non configurable default attributes
collections_directory: str = "collections"
array_data_directory: str = "array_data"
varray_data_directory: str = "varray_data"
array_symlinks_directory: str = "array_symlinks"
varray_symlinks_directory: str = "varray_symlinks"


@pytest.mark.parametrize(
    ("params", "exception"),
    [
        (
            {
                "workers": 1,
                "write_lock_timeout": 2,
                "write_lock_check_interval": 1,
                "memory_limit": 1,
            },
            TypeError,
        ),
        (
            {
                "uri": "",
                "workers": 1,
                "write_lock_timeout": 2,
                "write_lock_check_interval": 1,
                "memory_limit": 1,
            },
            None,
        ),
        (
            {
                "uri": 34,
                "workers": 1,
                "write_lock_timeout": 2,
                "write_lock_check_interval": 1,
                "memory_limit": 1,
            },
            ValueError,
        ),
    ],
)
def test_config_without_env(params: dict, exception: Exception):
    if exception:
        with pytest.raises(exception):
            DekerConfig(**params)
    else:
        config = DekerConfig(**params)
        assert config
        for key in params:
            assert params[key] == getattr(config, key)


if __name__ == "__main__":
    pytest.main(["--random-order"], ["pytest_random_order"])


def test_config_as_dict_lists_every_field():
    params = {
        "uri": "file:///tmp/deker",
        "workers": 2,
        "write_lock_timeout": 3,
        "write_lock_check_interval": 1,
        "memory_limit": 1024,
    }
    config = DekerConfig(**params)
    as_dict = config.as_dict
    assert isinstance(as_dict, dict)
    for key, value in params.items():
        assert as_dict[key] == value
    assert set(as_dict) == {field.name for field in fields(DekerConfig)}
    assert as_dict["collections_directory"] == "collections"
    assert as_dict["loglevel"] == "DEBUG"
