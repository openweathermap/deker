# deker - multidimensional arrays storage engine
# Copyright (C) 2023  OpenWeather
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

from __future__ import annotations

from collections import OrderedDict, namedtuple
from pathlib import Path
from typing import List, Optional, Tuple, Union
from urllib.parse import ParseResult, _NetlocResultMixinStr, parse_qs, quote, urlparse

from deker_tools.path import is_path_valid

from deker.errors import DekerValidationError


ParseResultWithServers = namedtuple("ParseResultWithServers", ParseResult._fields + ("servers",))


class Uri(ParseResultWithServers, _NetlocResultMixinStr):
    """Deker client uri wrapper.

    Overrides parsed result to have query as a dictionary.
    Validation ref: https://t.ly/ekbU
    """

    __annotations__ = OrderedDict(
        scheme={"separator": None, "divider": None},
        netloc={"separator": "://", "divider": None},
        servers={"separator": "@", "divider": ","},
        path={"separator": "/", "divider": "/"},
        params={"separator": ";", "divider": ","},
        query={"separator": "?", "divider": "&"},
        fragment={"separator": "#", "divider": None},
    )

    @property
    def raw_url(self) -> str:
        """Get url from raw uri without query string, arguments and fragments."""
        url = self.scheme + self.__annotations__["netloc"]["separator"]  # type: ignore[attr-defined]
        if self.netloc:  # type: ignore[attr-defined]
            url += self.netloc  # type: ignore[attr-defined]
        url += quote(
            str(self.path), safe=self.__annotations__["netloc"]["separator"][:-1]  # type: ignore[attr-defined]
        )
        return url

    @classmethod
    def __get_servers_and_netloc(cls, netloc: str, scheme: str) -> Tuple[str, Optional[List[str]]]:
        """Parse list of servers.

        :param netloc: Netloc object
        :param scheme: http or https
        """
        # If scheme is not http or https, it cannot work in cluster mode
        if (
            scheme not in ["http", "https"]
            or cls.__annotations__["servers"]["divider"] not in netloc
        ):
            # So servers will be None
            return netloc, None

        # Otherwise parse servers
        servers = netloc.split(cls.__annotations__["servers"]["divider"])
        node_with_possible_auth = servers[0]
        if cls.__annotations__["servers"]["separator"] in node_with_possible_auth:
            auth, _ = node_with_possible_auth.split(cls.__annotations__["servers"]["separator"])
            return (
                node_with_possible_auth,
                [
                    f"{scheme}{cls.__annotations__['netloc']['separator']}"
                    f"{auth}"
                    f"{cls.__annotations__['servers']['separator']}"
                    f"{host}"
                    for host in servers[1:]
                ],
            )
        return (
            node_with_possible_auth,
            [f"{scheme}{cls.__annotations__['netloc']['separator']}{host}" for host in servers[1:]],
        )

    @classmethod
    def __parse(cls, uri: str) -> Uri:
        """Parse uri from string.

        :param uri: : scheme://username:password@host:port/path;parameters?query
        """
        result = urlparse(uri)

        if ";" in result.path:
            path, params = result.path.split(cls.__annotations__["params"]["separator"])
        else:
            path, params = result.path, result.params

        query = parse_qs(result.query)
        netloc, servers = cls.__get_servers_and_netloc(result.netloc, result.scheme)
        return Uri(  # type: ignore
            result.scheme,
            netloc,
            path,  # type: ignore[arg-type]
            params,
            query,  # type: ignore[arg-type]
            result.fragment,
            servers,
        )

    @classmethod
    def validate(cls, uri: str) -> None:
        """Validate uri from string.

        :param uri: : scheme://username:password@host:port/path;parameters?query
        """
        if not isinstance(uri, str):
            raise DekerValidationError(f"Invalid uri type: {uri} - {type(uri)}")
        if not uri or uri.isspace():
            raise DekerValidationError("Empty uri passed")

        parsed_uri = cls.__parse(uri)
        if parsed_uri.scheme == "file":  # type: ignore[attr-defined]
            pathname = Path(parsed_uri.path)  # type: ignore[attr-defined]
            try:
                is_path_valid(pathname)
            except Exception as e:
                raise DekerValidationError(f"Invalid uri: {e}; {uri}")

    @classmethod
    def create(cls, uri: str) -> Uri:
        """Create parsed and validated uri from string.

        :param uri: scheme://username:password@host:port/path;parameters?query
        """
        cls.validate(uri)
        return cls.__parse(uri)

    def __truediv__(self, other: Union[str, Path]) -> Uri:
        """Join uri to Path or string.

        :param other: Path or string to join
        """
        sep = self.__annotations__["path"]["separator"]
        other = str(other)
        path = sep.join((self.path, other.strip()))  # type: ignore[attr-defined]
        netloc, servers = self.__get_servers_and_netloc(self.netloc, self.scheme)  # type: ignore[attr-defined]
        res = Uri(  # type: ignore
            self.scheme,  # type: ignore[attr-defined]
            netloc,
            path,  # type: ignore[attr-defined]
            self.params,  # type: ignore[attr-defined]
            self.query,  # type: ignore[attr-defined]
            self.fragment,  # type: ignore[attr-defined]
            servers,
        )
        return res

    def __itruediv__(self, other: str) -> Uri:
        return self.__truediv__(other)
