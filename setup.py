"""Package setup."""

import os
import re
import sys

from typing import Optional

from setuptools import find_packages, setup


PACKAGE_NAME: str = "deker"


def get_version() -> str:
    """Get version from commit tag.

    Regexp reference:
    https://gitlab.openweathermap.org/help/user/packages/pypi_repository/index.md#ensure-your-version-string-is-valid
    """
    ci_commit_tag: Optional[str] = os.getenv("CI_COMMIT_TAG", "0.0.0")
    regex = (
        r"(?:"
        r"(?:([0-9]+)!)?"
        r"([0-9]+(?:\.[0-9]+)*)"
        r"([-_\.]?((a|b|c|rc|alpha|beta|pre|preview))[-_\.]?([0-9]+)?)?"
        r"((?:-([0-9]+))|(?:[-_\.]?(post|rev|r)[-_\.]?([0-9]+)?))?"
        r"([-_\.]?(dev)[-_\.]?([0-9]+)?)?"
        r"(?:\+([a-z0-9]+(?:[-_\.][a-z0-9]+)*))?"
        r")$"
    )
    try:
        return re.search(regex, ci_commit_tag, re.X + re.IGNORECASE).group()
    except Exception:
        sys.exit(f"No valid version could be found in CI commit tag {ci_commit_tag}")


with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [
        line.strip("\n")
        for line in f
        if line.strip("\n") and not line.startswith(("#", "-i", "abstract"))
    ]


setup_kwargs = dict(
    name=PACKAGE_NAME,
    version=get_version(),
    author="Matvey Vargasov, Sergey Rybakov, Iakov Matyushin",
    author_email="mvargasov@openweathermap.org, srybakov@openweathermap.org, imatiushin@openweathermap.org",
    description="",
    packages=find_packages(exclude=["examples", "tests", "test*.*"]),
    package_data={PACKAGE_NAME: ["py.typed"]},
    include_package_data=True,
    platforms="any",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "xarray": ["xarray>=2023.5.0"],
    },
)
setup(**setup_kwargs)
