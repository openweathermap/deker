import logging
import multiprocessing
import os
import platform
import shutil

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from multiprocessing.context import BaseContext
from pathlib import Path
from random import choices
from string import ascii_letters

import pytest

from tests.parameters.uri import embedded_uri

from deker.ABC import BaseStorageAdapter
from deker.client import Client
from deker.config import DekerConfig
from deker.ctx import CTX
from deker.log import _ROOT_DEKER_LOGGER_NAME
from deker.uri import Uri

logging.basicConfig(level="ERROR")

pytest_plugins = [
    "tests.fixtures.collection",
    "tests.fixtures.locks",
    "tests.fixtures.subset",
    "tests.fixtures.adapter",
    "tests.fixtures.schema",
    "tests.fixtures.arrays",
    "tests.fixtures.factory",
    "tests.fixtures.manager",
]


@pytest.fixture(scope="class")
def workers() -> int:
    """Get reduced number of threads."""
    return cpu_count() // 2


@pytest.fixture(scope="class")
def executor(workers: int) -> ThreadPoolExecutor:
    """Creates thread executor."""
    return ThreadPoolExecutor(workers)


@pytest.fixture(scope="class")
def config(root_path: Path, workers) -> DekerConfig:
    """Deker config for testing env."""
    return DekerConfig(
        uri=embedded_uri(root_path),
        workers=workers,
        collections_directory="collections",
        array_data_directory="array_data",
        varray_data_directory="varray_data",
        array_symlinks_directory="array_symlinks",
        varray_symlinks_directory="varray_symlinks",
        read_timeout=60,
        write_lock_timeout=60,
        write_lock_check_interval=1,
        memory_limit=104857600,
        loglevel="ERROR",
    )


@pytest.fixture
def deker_log(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """Capture the records of the Deker loggers, which do not propagate to the root logger.

    The records of every level are captured: the fixture lowers the level of the Deker loggers for the test.

    :param caplog: pytest log capture
    :returns: the same capture, fed by the Deker loggers
    """
    logger = logging.getLogger(_ROOT_DEKER_LOGGER_NAME)
    loggers = [
        logger,
        *(child for name, child in logger.manager.loggerDict.items() if name.startswith(f"{logger.name}.")),
    ]
    levels = {child: child.level for child in loggers if isinstance(child, logging.Logger)}
    for child in levels:
        child.setLevel(logging.DEBUG)
    logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)
        for child, level in levels.items():
            child.setLevel(level)


@pytest.fixture
def name() -> str:
    """Generates random 10 chars name."""
    return "".join(choices(ascii_letters, k=10))


@pytest.fixture(scope="session")
def root_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Returns root of application.

    :param tmp_path_factory:  Temporary directory factory
    """
    if path := os.getenv("ROOT_PATH"):
        directory = Path(path)
    else:
        directory = tmp_path_factory.mktemp("test", numbered=False)
    yield directory
    if platform.system().lower() == "linux":
        path = directory.parents[0]
        if directory.exists():
            shutil.rmtree(path)


@pytest.fixture(scope="class")
def uri(root_path) -> Uri:
    """Creates Uri instance from string."""
    return Uri.create(embedded_uri(root_path))


@pytest.fixture(scope="class")
def ctx(uri: Uri, config, storage_adapter, workers) -> CTX:
    """Creates Client context."""
    return CTX(uri, config, storage_adapter, ThreadPoolExecutor(workers))


@pytest.fixture(scope="class")
def client(
    root_path: Path,
    storage_adapter: type[BaseStorageAdapter],
    workers: int,
) -> Client:
    """Creates Client object and init all connections.

    :param root_path: path to directory with collections fixture
    :param storage_adapter: BaseStorageAdapter fixture
    :param workers: Number of workers for pool
    """
    with Client(
        uri=str(embedded_uri(root_path)),
        workers=workers,
        loglevel="ERROR",
    ) as client:
        yield client


@pytest.fixture(params=multiprocessing.get_all_start_methods())
def mp_context(request: pytest.FixtureRequest) -> BaseContext:
    """Multiprocessing context for every way of starting processes on this platform.

    Linux: fork, spawn, forkserver; macOS and Windows: without fork.
    Tests shall take Process, Pool and Manager from the context, not from the multiprocessing module,
    so each test runs once per start method and its id shows which one failed.

    :param request: pytest request with the start method name in ``param``
    :returns: multiprocessing context of the requested start method
    """
    return multiprocessing.get_context(request.param)
