import sys

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count

from deker.config import DekerConfig


sys.path.insert(0, ".")

import argparse
import asyncio
import time

from pathlib import Path

from deker.errors import DekerLockError
from deker.locks import Flock


path = Path("/tmp/mp_test/collections/coll.lock")
ex = ThreadPoolExecutor(max_workers=cpu_count() // 2)
config = DekerConfig(
    collections_directory="collections",
    array_data_directory="array_data",
    varray_data_directory="varray_data",
    array_symlinks_directory="array_symlinks",
    varray_symlinks_directory="varray_symlinks",
    uri="/tmp/mp_test/",
    graceful_shutdown=True,
)


def async_method():
    flock = Flock(path)
    asyncio.sleep(1)
    flock.acquire()
    asyncio.sleep(1)
    flock.release()


def sync_method():
    flock = Flock(path)
    time.sleep(1)
    flock.acquire()
    time.sleep(1)
    flock.release()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--command")
    args = parser.parse_args()
    try:
        if args.command == "sync":
            sync_method()
        elif args.command == "async":
            asyncio.run(async_method())
    except DekerLockError as e:
        print(repr(e))
        sys.exit(1)
