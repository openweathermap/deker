import sys


sys.path.insert(0, ".")

import argparse
import asyncio
import os
import time

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from pathlib import Path

from deker.config import DekerConfig
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
)

flock = Flock(path)
pid = os.getpid()


def async_method(start):
    try:
        flock.acquire()
        print(pid, "Flock acquired", flush=True)
        while True:
            # print(pid)
            asyncio.sleep(0.2)
    finally:
        flock.release()
        print(pid, time.monotonic() - start, flush=True)


def sync_method(start):
    try:
        flock.acquire()
        print(pid, "Flock acquired", flush=True)
        while True:
            # print(pid)
            time.sleep(0.2)
    finally:
        flock.release()
        print(pid, time.monotonic() - start, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--command", default="sync")
    args = parser.parse_args()
    start = time.monotonic()
    try:
        if args.command == "sync":
            sync_method(start)
        elif args.command == "async":
            asyncio.run(async_method(start))
    except:
        pass
