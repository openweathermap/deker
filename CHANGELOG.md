# Changelog

All notable changes to DEKER™ are listed here, release by release, newest first. Only production
releases are listed; the beta builds that precede them are not. The satellite packages keep their
own changelogs: [deker-local-adapters](https://github.com/openweathermap/deker-local-adapters/blob/main/CHANGELOG.md),
[deker-server-adapters](https://github.com/openweathermap/deker-server-adapters/blob/main/CHANGELOG.md),
[deker-shell](https://github.com/openweathermap/deker-shell/blob/main/CHANGELOG.md) and
[deker-tools](https://github.com/openweathermap/deker-tools/blob/main/CHANGELOG.md).

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), the versions follow
[Semantic Versioning](https://semver.org/).

## [2.0.0]

A major release. The locking protocol changed, so every process working with one storage must be
updated at once, and the satellite packages are released together with it: `deker-local-adapters`,
`deker-server-adapters`, `deker-shell` and `deker-tools` 2.0.0.

### Breaking changes

- Locks work without marker files. A reader holds a shared lock on the file of the array for the
  whole read; a writer takes an exclusive lock and waits for the readers to leave (up to
  `write_lock_timeout`). The locks live in the operating system and are released when a process
  dies, so a killed reader or writer blocks nobody and leaves nothing on the disk. The locks work
  between processes and between containers sharing a volume on one machine; they do not work over
  NFS. All processes working with one storage must run 2.0.0.
- A VArray has no lock of its own anymore: it is written Array by Array, each under the usual
  Array write lock. Writers of different Arrays do not wait for each other, writers of one Array
  queue up. If one Array could not be locked in time, the write raises `DekerVSubsetError` listing
  the causes, while the other Arrays are already written.
- Writing data follows one rule. Python numbers, lists and tuples are converted to the dtype of the
  collection when every value fits: integers exactly, floats up to rounding, complex numbers by
  both parts. NumPy arrays and NumPy scalars are never converted: their dtype must be the dtype of
  the collection. Everything else raises `DekerValidationError` before anything is written.
- Dimension labels are validated by the schema: a non-empty list or tuple with one label per cell,
  all strings or all floats, without duplicates. Integers, booleans, dicts and mixed labels are
  rejected.
- Filtering by anything but the id or all the primary attributes raises `DekerFilterError` instead of
  `NotImplementedError`.
- `Client.check_integrity` has a new signature: `check_integrity(collection=None, checks=
  DEFAULT_INTEGRITY_CHECKS, stop_on_error=False, repair=False, workers=1, to_file=False)` and
  returns an `IntegrityReport`. The `level` argument is gone, `stop_on_error` is off by default.
- The types are precise. `Collection[Array]` and `Collection[VArray]` are distinct types,
  `Client.create_collection` picks one by the schema and `Client.get_collection` returns one of them
  or `None`. The adapter interfaces are generic over the path type (`Path` or `Uri`), the array type
  and the storage options; `create()` of an adapter returns the created array. Code implementing
  the adapter interfaces must follow.
- A closed client raises `DekerClientError` from every method; the adapters of a closed context
  raise `DekerAdapterClosedError`.
- The dependencies are pinned to the new majors: `deker-tools>=2,<3`, `deker-local-adapters>=2,<3`
  and, in the extras, `deker-server-adapters>=2,<3` and `deker-shell>=2,<3`.

### Added

- Steps in slices: `array[::2]` and `array[::-1]` work for Array and VArray, in any number of
  dimensions. On a time dimension the step may be a `timedelta` multiple of the dimension step, on
  a scaled dimension a float multiple of the scale step; a labelled dimension takes integer steps
  only. Zero and booleans are rejected. `describe()` and the export to xarray follow the step.
- Writing a number into a slice of a VArray fills the whole slice with it, as it does for an Array.
- The integrity check is rewritten: six named checks (`collections`, `markers`, `arrays`, `links`,
  `data`, `counters`; the first four run by default and do not read the data), a report with a
  finding per problem (check, message, collection, array, path, whether it is repairable),
  `repair=True` to fix what can be fixed (symlinks, lock files, stale markers, the start time of
  Arrays, the empty-cell counters) and `workers` for parallel collections. An outdated collection
  metadata file is reported with the diff of the fields and is not rewritten.
- `py.typed` in the package: the types are checked by the users of DEKER™ too.
- Documentation: the Locks and Step sections rewritten, the Integrity check section added, a
  Changelog page, and the documentation site keeps every released version with a version switcher.

### Changed

- Reading and writing cost less: the free memory of the machine is queried only for a request above 1% of
  the memory limit of the client (a smaller one fits anyway, and the query read the system tables on every
  access), the dimensions of an array are built once instead of on every access, and the messages of the
  log are formatted only when they are written. Reading one Array through a VArray takes about a third
  less, reading an Array directly about 40% less.
- Creating a missing Array inside a VArray is serialised: between processes by a lock on the
  file of the VArray, between threads by a thread lock; the loser writes into the Array the winner
  created. Creating an Array or a VArray with primary attributes holds a lock on the directory of
  its symlink, and the files are created in the create-only mode: a second creation of the same
  array raises `DekerValidationError` instead of leaving two files for one position.
- The start time of a new Array inside a VArray with a time dimension is computed correctly:
  start + step × Array length × position. The Arrays written by 1.x with the wrong value are found
  and fixed by `client.check_integrity(checks=[IntegrityCheck.arrays], repair=True)`.
- `Flock.acquire` raises `FileNotFoundError` instead of pretending the lock is taken when the file
  does not exist.
- The library does not print to stdout: the integrity report goes to the logger at the error
  level, the statuses at the info level, and the list of locks is returned, not printed.
- A client dropped without `close()` is closed by its finalizer silently; `close()` logs as before.
- Creating a client sets the format of the DEKER™ loggers only; the handlers of the other loggers of the
  application and the root logger keep their formats (before, every handler of the process was
  reformatted and the root logger configured).
- An update of the custom attributes of a `VArray` and the creation of its `Arrays` lock the directory of
  the `VArray` file, not the file itself, which is replaced by the update; the update waits for the
  previous one like a write does.
- The errors of the adapters go to the DEKER™ logger, not to the root logger.
- `DekerVSubsetError` says how many Arrays failed and that the others are written, instead of warning
  about corrupted data.

### Fixed

- DEKER™ installs and works on Apple silicon macOS and on Windows: `float128` and `complex256` are taken
  from NumPy only where NumPy provides them, instead of failing at import. These two dtypes are not
  available there, and a collection created with one of them elsewhere cannot be opened there:
  `DekerMetaDataError` says that the dtype is not available on the platform.
- A writer could start writing a file that was still being read: the reader released its lock
  after the header and marked the read with a file created later. The window is gone with the
  marker files.
- Writing to a VArray hung for a minute, and then failed, when any of its Arrays was being read,
  and the readers of the Arrays it had taken got errors for that minute.
- Two processes writing into a new VArray at once could create two files for one position of it,
  and the data of one of them was lost.
- A foreign write marker was taken for an own one when the process id of the writer ended with the
  own one (process 23 accepted the marker of process 5123).
- Reading a VArray with a step collected the result from the wrong cells; an ellipsis in an index
  expanded to one dimension instead of all the missing ones.
- `check_integrity(to_file=True)` wrote the report and then crashed.
- An integer index beyond the axis (`array[10]` on an axis of 10, `array[-11]`) was accepted and read as
  fill values; it raises `IndexError`, as in NumPy.
- Small bugs found while bringing the coverage to 100%: the attributes were lost on an update,
  `None` inside attribute tuples was rejected, the errors of an attribute schema with a wrong field
  and of a filter by an invalid id were unreadable.

### Deprecated

- `Client.clear_locks` is kept for one purpose: to remove the marker files of 1.x once after the
  upgrade. It warns and is removed in 3.0.0, together with the `markers` integrity check and the
  marker values of the lock enums.

### Removed

- The read markers, the write markers of a VArray (the `.varraylock` files), the process token
  and `WriteVarrayLock`.
- `check_integrity(level=...)` and the integrity checker classes of 1.x.
- The unused types `ArrayOffset`, `ArraysCoordinatesWithOffset`, `ArraysInDimension`, `ArraysCoordinates`
  and `Collection.__next__`.
- `from __future__ import annotations` is banned in the package: the validators and the schemas
  read the annotations at runtime.

### Internals and tooling

- The build moved from poetry to uv, the version comes from the git tag; ruff replaces black,
  flake8, isort and pycln, pydoclint replaces darglint; a Makefile with `make ci`,
  `make test-matrix` (every supported Python and 3.15) and `make install-matrix`; pre-commit with
  the type check running in the project environment.
- 100% test coverage is required, every line by a test of its own module; the tests run in random
  order and do not depend on it; hung tests fail by the `pytest-timeout` plugin instead of hanging
  the run; the nested functions of `VSubset` became methods.
- The CI workflows (deker-actions) run `make check` and `make coverage` on every supported Python,
  build the wheel along with the source distribution and publish the versioned documentation.

## [1.2.0] - 2026-11-11

### Added

- Python 3.13 and 3.14 are supported.
- NumPy 2 is supported (`numpy>=1.26,<3`). `DTypeEnum` keeps all its member names: the NumPy
  aliases removed in NumPy 2 (`cfloat`, `complex_`, `longfloat` and so on) map to their canonical
  types, so existing schemas keep working.

### Changed

- The dependency ranges are widened to the current majors: `attrs>=23.1,<27`, `psutil>=5.9.5,<8`,
  `xarray>=2024.6.0`; the dev and docs tooling is updated.
- CI runs the tests on every supported Python.
- Documentation: the installation requirements and the dtype table are updated.

### Removed

- Python 3.9. Pin `deker<1.2` to stay on it.

## [1.1.7] - 2024-07-03

### Added

- `Client(skip_collection_create_memory_check=True)` skips the memory check when a collection is
  created; the check moved from the collection to the adapter.

### Changed

- `deker-local-adapters>=1.1.2`.

## [1.1.6] - 2024-05-08

### Changed

- `DekerVSubsetError` is an exception group (`DekerExceptionGroup`): the errors of the threads
  writing a VArray are collected and reported together.
- The primary and custom attributes keep the order of the schema (`make_ordered_dict`), and the
  attribute values are serialised in one place.
- The adapters' `create` methods return nothing anymore; `CreateArrayLock` is removed.

### Fixed

- The attributes were invoked wrongly when an array was created from its metadata.

## [1.1.5] - 2024-04-03

### Fixed

- A lock on a file that did not exist yet.
- The log messages were duplicated: the DEKER™ logger does not propagate to the root logger
  anymore, and the `DEKER_LOGLEVEL` environment variable is not read.

## [1.1.4] - 2024-03-05

### Fixed

- Two different arrays could get the same id: the ids are `uuid4` instead of `uuid5`.
- The integrity checker.

## [1.1.3] - 2023-12-29

### Added

- Unsigned integer dtypes; NumPy type aliases in `DTypeEnum`.

### Changed

- `WriteVarrayLock` does not lock the metadata file anymore; the locks of the Arrays of
  intersecting VArray subsets are handled correctly.
- A clearer `DekerMemoryError` message.
- The documentation is extended.

### Fixed

- The locks of `WriteVarrayLock` and `CreateArrayLock`.
- The maximum of the NumPy integer types.

## [1.1.2] - 2023-11-20

### Added

- `create()` accepts the id of the Array or the VArray to create.
- A URI may list several servers (`http://host1,host2,host3`): the cluster mode of the server
  adapters.
- `memory_limit` accepts a plain number of bytes.

### Fixed

- An empty attributes schema raised a wrong error.
- The validation of a collection and of the custom attributes moved to dedicated methods.

## [1.1.1] - 2023-08-31

### Fixed

- The close of the client, the factory and the executor tolerate a partially constructed object.
- The rounding of the scaled dimension values is taken from the step, not from the start value.
- The error classes are defined for every failure of the client.

## [1.1.0] - 2023-08-18

### Added

- `VArraySchema(arrays_shape=...)`: the shape of the Arrays a VArray is split into, as an
  alternative to `vgrid`.
- `Client.create_collection_from_dict`.

### Changed

- `BaseArray._create_from_meta` replaces `create_array_from_meta`; the grid splitters are
  validated by `VArraySchema` only; `SchemaTypeEnum` moved to `deker.schemas`.
- The documentation is restructured; all the configuration lives in `pyproject.toml`; the CI
  workflows moved to deker-actions.

## [1.0.0] - 2023-08-01

The pilot release of DEKER™.

[2.0.0]: https://github.com/openweathermap/deker/compare/v1.2.0...v2.0.0
[1.2.0]: https://github.com/openweathermap/deker/compare/v1.1.7...v1.2.0
[1.1.7]: https://github.com/openweathermap/deker/compare/v1.1.6...v1.1.7
[1.1.6]: https://github.com/openweathermap/deker/compare/v1.1.5...v1.1.6
[1.1.5]: https://github.com/openweathermap/deker/compare/v1.1.4...v1.1.5
[1.1.4]: https://github.com/openweathermap/deker/compare/v1.1.3...v1.1.4
[1.1.3]: https://github.com/openweathermap/deker/compare/v1.1.2...v1.1.3
[1.1.2]: https://github.com/openweathermap/deker/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/openweathermap/deker/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/openweathermap/deker/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/openweathermap/deker/releases/tag/v1.0.0
