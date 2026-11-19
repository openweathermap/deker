# deker - multidimensional arrays storage engine
# Copyright (C) 2023-2026  Openweather Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple, Union

from deker.errors import DekerValidationError
from deker.log import SelfLoggerMixin
from deker.types.public.enums import IntegrityCheck, IntegrityFindingKind

Labels = Union[tuple[Union[str, float], ...], list[Union[str, float]]]


class Scale(NamedTuple):
    """NamedTuple with a description of a dimension regular scale.

    For example we describe dimensions for the Earth's latitude and longitude grades::

        dims = [
                DimensionSchema(name="y", size=721),
                DimensionSchema(name="x", size=1440),
            ]

    Such description may exist, but it's not quite sufficient. We can provide more information for the grid::

        dims = [
                    DimensionSchema(
                        name="y",
                        size=721,
                        scale=Scale(start_value=90, step=-0.25, name="lat")
                    ),
                    DimensionSchema(
                        name="x",
                        size=1440,
                        scale=Scale(start_value=-180, step=0.25, name="lon")
                    ),
                ]

    This extra information permits us provide fancy indexing by lat/lon coordinates in degrees::

        EarthGridArray[1, 1] == EarthGridArray[89.75, -179.75]

    The value of the cell ``i`` of the dimension is ``start_value + step * i``.

    :param start_value: scale real start point
    :param step: scale values step (may be positive or negative)
    :param name: optional scale name (latitude, longitude or whatever)
    """

    start_value: float
    step: float
    name: str | None = None


class StorageSize(NamedTuple):
    """Namedtuple for storage size with bytes and human representation.

    :param bytes: size in bytes
    :param human: the same size as a string with a unit, e.g. ``"1.5 GB"``
    """

    bytes: int
    human: str


class IndexLabels(tuple, SelfLoggerMixin):
    """``IndexLabels`` is an overloaded ``tuple`` providing a mapping of label-values to their ``Dimension`` indexes.

    It provides direct and reversed ordered mappings of dimension's values names (labels)
    to their position in the axis flat array (indexes).

    Labels shall be a sequence of unique values within the full scope of the passed ``Dimension``.
    Valid Labels are list or tuple of unique strings or floats::

      ["name1", "name2", ..., "nameN"] | (0.2, 0.1, 0.4, 0.3, ..., -256.78963)
    """

    @classmethod
    def __validate(cls, labels: Labels | None) -> tuple[str | float, ...]:
        """Check the labels and return them as a tuple.

        :param labels: labels
        :returns: the labels as a tuple
        :raises DekerValidationError: if the labels are empty or not all strings or all floats
        """
        error = DekerValidationError(
            "Labels shall be a list or tuple of unique values of the same type (strings, floats), "
            "not None or empty list or tuple"
        )
        if (
            not labels
            or not isinstance(labels, (list, tuple))
            or all(
                (
                    any(not isinstance(val, str) for val in labels),
                    any(not isinstance(val, float) for val in labels),
                )
            )
        ):
            raise error
        return labels if isinstance(labels, tuple) else tuple(labels)

    def __new__(cls, labels: Labels | None) -> "IndexLabels":
        """Override python ``tuple.__new__`` method.

        Here we add values validation before an instance is created.

        :param labels: labels
        :returns: validated ``IndexLabels`` instance
        """
        return super().__new__(cls, cls.__validate(labels))

    def __init__(self, labels: Labels | None) -> None:  # noqa: ARG002
        """Log the creation; the labels are set by ``__new__``.

        :param labels: list or tuple of unique strings or floats
        """
        self.logger.debug("labels initialized")

    @property
    def first(self) -> str | float:
        """Get first label."""
        return self[0]

    @property
    def last(self) -> str | float:
        """Get last label."""
        return self[-1]

    def name_to_index(self, name: str | float) -> int | None:
        """Get label index by its name.

        :param name: label name
        :returns: index of the label, or ``None`` if there is no such label
        """
        try:
            return self.index(name)
        except ValueError as e:
            mes = str(e) + f": no name {name}"
            self.logger.debug(mes)
            return None

    def index_to_name(self, idx: int) -> str | float | None:
        """Get label name by its index.

        :param idx: label index
        :returns: label at the index, or ``None`` if the index is out of range
        """
        try:
            return self[idx]
        except IndexError as e:
            mes = str(e) + f": no index {idx}"
            self.logger.debug(mes)
            return None

    def __str__(self) -> str:
        return str(tuple(self))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"


@dataclass
class IntegrityFinding:
    """One problem found by an integrity check.

    :param check: the check that found it
    :param kind: what exactly was found
    :param message: the problem in words
    :param collection: name of the collection, ``None`` for the storage itself
    :param array_id: id of the Array or VArray, if the finding is about one
    :param path: the file or directory the finding is about
    :param repairable: whether the check knows how to repair it
    :param repaired: whether it has been repaired
    :param repair_error: why the repair failed, empty if it did not
    """

    check: IntegrityCheck
    kind: IntegrityFindingKind
    message: str
    collection: str | None = None
    array_id: str | None = None
    path: Path | None = None
    repairable: bool = False
    repaired: bool = False
    repair_error: str = ""


@dataclass
class IntegrityReport:
    """Result of an integrity check: the findings, grouped by collection and check in the text.

    :param findings: everything the checks found, in the order of finding
    """

    findings: list[IntegrityFinding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Whether nothing was found."""
        return not self.findings

    @property
    def unrepaired(self) -> list[IntegrityFinding]:
        """The findings that are still there: not repairable, not repaired or failed to repair."""
        return [finding for finding in self.findings if not finding.repaired]

    def by_check(self, check: IntegrityCheck) -> list[IntegrityFinding]:
        """Findings of one check.

        :param check: the check
        :returns: its findings, in the order of finding
        """
        return [finding for finding in self.findings if finding.check == check]

    @staticmethod
    def _line(finding: IntegrityFinding) -> str:
        """One line of the text for a finding.

        :param finding: the finding
        :returns: its message with the outcome of the repair
        """
        if finding.repaired:
            return f"{finding.message} (repaired)"
        if finding.repair_error:
            return f"{finding.message} (repair failed: {finding.repair_error})"
        return finding.message

    def __str__(self) -> str:
        groups: dict[tuple[str | None, IntegrityCheck], list[str]] = defaultdict(list)
        for finding in self.findings:
            groups[(finding.collection, finding.check)].append(self._line(finding))
        text = ""
        for (collection, check), lines in groups.items():
            place = "Storage" if collection is None else f"Collection {collection}"
            text += f"{place}, {check.value}:\n" + "".join(f"\t- {line}\n" for line in lines)
        return text
