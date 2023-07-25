class DekerBaseApplicationError(Exception):
    """Base attribute exception."""


class DekerClientError(DekerBaseApplicationError, RuntimeError):
    """If something goes wrong in Client."""

    pass


class DekerArrayError(DekerBaseApplicationError):
    """If something goes wrong in Array."""


class DekerArrayTypeError(DekerArrayError):
    """If final (V)Array's values type do not match the dtype, set in Collection schema."""

    pass


class DekerCollectionError(DekerBaseApplicationError):
    """If something goes wrong in Collection."""

    pass


class DekerCollectionAlreadyExistsError(DekerCollectionError):
    """If collection with the same name already exists."""

    pass


class DekerCollectionNotExistsError(DekerCollectionError):
    """If collection doesn't exist."""

    pass


class DekerInvalidSchemaError(DekerBaseApplicationError):
    """If schema is invalid."""

    pass


class DekerMetaDataError(DekerInvalidSchemaError):
    """If metadata is invalid/corrupted."""

    pass


class DekerFilterError(DekerBaseApplicationError):
    """If something goes wrong during filtering."""

    pass


class DekerValidationError(DekerBaseApplicationError):
    """If something goes wrong during validation."""

    pass


class DekerIntegrityError(DekerBaseApplicationError):
    """If something goes wrong during integrity check."""

    pass


class DekerLockError(DekerBaseApplicationError):
    """If a Collection or a (V)Array instance is locked."""

    pass


class DekerInstanceNotExistsError(DekerBaseApplicationError):
    """If instance was deleted, but user is trying to call methods on it."""

    pass


class DekerWarning(Warning):
    """Deker warnings."""

    pass


class DekerInvalidManagerCallError(DekerBaseApplicationError):
    """If we try to call method that shouldn't be called.

    E.g., .varrays in Arrays Collection
    """


class DekerSubsetError(DekerArrayError):
    """If something goes wrong while Subset managing."""


class DekerVSubsetError(DekerSubsetError):
    """If something goes wrong while VSubset managing."""


class DekerMemoryError(DekerBaseApplicationError, MemoryError):
    """Early memory overflow exception."""
