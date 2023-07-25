from attr import asdict, dataclass, fields


@dataclass(kw_only=True)
class DekerConfig:
    """Deker application configuration.

    :param uri: Deker uri string
    :param workers: number of threads for VArray management
    :param write_lock_timeout: number of seconds for WriteLock timeout
    :param write_lock_check_interval: number of seconds for WriteLock check
    :param memory_limit: RAM size in bytes available for Deker
    :param loglevel: Deker logging level
    """

    # configurable attributes
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

    @property
    def as_dict(self) -> dict:
        """Serialize as dict."""
        return asdict(self)

    def __attrs_post_init__(self) -> None:
        """Check if types are correct."""

        for field in fields(self.__class__):  # type: ignore[arg-type]
            default_value = getattr(self, field.name)
            if not isinstance(default_value, field.type):
                raise ValueError(f"'{field.name}' setting has wrong type")
