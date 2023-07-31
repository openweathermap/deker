from abc import ABC, abstractmethod
from typing import Any, Optional

from deker.types.private.classes import Serializer


class BaseCollectionOptions(Serializer, ABC):
    """Base interface for collection options.

    Options, such as chunks, compression -
    or whatever that may somehow influence storage files' size or structure -
    depend on provided storage adapter
    """

    chunks: Any
    compression_opts: Any

    @classmethod
    @abstractmethod
    def _process_options(cls, storage_options: Optional[dict]) -> dict:
        """Validate and convert collection storage options.

        :param storage_options: options for storing data by a certain storage adapter,
        like chunks, compression, etc. Such options are passed to a collection to be used for every
        Array in it.
        """
        pass
