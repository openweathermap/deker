from collections import defaultdict
from pathlib import Path

import pytest

from deker.client import Client
from deker.ctx import CTX
from deker.integrity import ArraysChecker, CollectionsChecker, DataChecker, PathsChecker


@pytest.fixture()
def data_checker(client: Client, root_path: Path, ctx: CTX) -> DataChecker:
    """Data integrity checker."""
    return DataChecker(
        True, {}, defaultdict(list), 4, client, root_path / ctx.config.collections_directory
    )


@pytest.fixture()
def paths_checker(
    client: Client,
    root_path: Path,
    data_checker: DataChecker,
) -> PathsChecker:
    """File system paths integrity checker."""
    checker = PathsChecker(True, {}, defaultdict(list), 4, client, data_checker.root_path)
    checker.next_checker = data_checker
    return checker


@pytest.fixture()
def arrays_checker(client: Client, root_path: Path, paths_checker: PathsChecker) -> ArraysChecker:
    """Arrays integrity checker."""
    checker = ArraysChecker(True, {}, defaultdict(list), 4, client, paths_checker.root_path)
    checker.next_checker = paths_checker
    return checker


@pytest.fixture()
def collections_checker(
    client: Client,
    root_path: Path,
    arrays_checker: ArraysChecker,
) -> CollectionsChecker:
    """Collections integrity checker."""
    checker = CollectionsChecker(True, {}, defaultdict(list), 4, client, arrays_checker.root_path)
    checker.next_checker = arrays_checker
    return checker
