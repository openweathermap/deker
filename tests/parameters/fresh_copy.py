"""Loads a module once more as a separate module: for tests of code that depends on the platform at import."""

import importlib.util

from types import ModuleType


def load_fresh_copy(module: ModuleType, name: str) -> ModuleType:
    """Execute the source of a module once more as a separate module, with the current global state.

    The copy is not registered in ``sys.modules``, so the module every other test imports stays as it is.

    :param module: the module to copy
    :param name: the name of the copy, in the package of the module so that its relative imports work
    :returns: the copy
    """
    spec = importlib.util.spec_from_file_location(name, module.__file__)
    assert spec is not None and spec.loader is not None
    copy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(copy)
    return copy
