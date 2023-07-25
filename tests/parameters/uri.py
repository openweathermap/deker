from pathlib import Path


def embedded_uri(path: Path) -> str:
    """Makes a file string from path.

    :param path: Path object
    """
    return f"file://{path.resolve()}"
