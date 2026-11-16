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


from collections import OrderedDict
from pathlib import Path
from typing import NamedTuple
from urllib.parse import _NetlocResultMixinStr, parse_qs, quote, urlparse

from deker_tools.path import is_path_valid

from deker.errors import DekerValidationError


class ParseResultWithServers(NamedTuple):
    """Result of uri parsing: ``urllib.parse.ParseResult`` fields plus the list of cluster servers.

    :param scheme: uri scheme, e.g. ``file`` or ``http``
    :param netloc: network location of the first server (with credentials and port, if any)
    :param path: uri path
    :param params: uri parameters
    :param query: query string parsed into a dictionary
    :param fragment: uri fragment
    :param servers: full uris of all cluster servers, or ``None`` for a single server
    """

    scheme: str
    netloc: str
    path: str
    params: str
    query: dict[str, list[str]]
    fragment: str
    servers: list[str] | None


class Uri(ParseResultWithServers, _NetlocResultMixinStr):
    """Deker client uri wrapper.

    Overrides parsed result to have query as a dictionary. A cluster uri lists its hosts with commas,
    ``http://user:pass@host1,host2``: ``netloc`` keeps the first host, ``servers`` the full uris of the others
    with the same credentials.
    Validation ref: https://t.ly/ekbU
    """

    NETLOC_SEPARATOR = "://"
    SERVERS_SEPARATOR = "@"  # separates credentials from a host: ``user:pass@host``
    SERVERS_DIVIDER = ","  # separates cluster hosts: ``host1,host2``
    PATH_SEPARATOR = "/"
    PARAMS_SEPARATOR = ";"

    # Backward compatibility: the delimiters used to be published under this (misused) name, and
    # deker-server-adapters < 1.3 read ``Uri.__annotations__["netloc"]["separator"]``.
    # Keep it the last statement of the class body: an annotated assignment below would be added to it.
    __annotations__ = OrderedDict(
        scheme={"separator": None, "divider": None},
        netloc={"separator": NETLOC_SEPARATOR, "divider": None},
        servers={"separator": SERVERS_SEPARATOR, "divider": SERVERS_DIVIDER},
        path={"separator": PATH_SEPARATOR, "divider": PATH_SEPARATOR},
        params={"separator": PARAMS_SEPARATOR, "divider": ","},
        query={"separator": "?", "divider": "&"},
        fragment={"separator": "#", "divider": None},
    )

    @property
    def raw_url(self) -> str:
        """Get the url of the first server without query string, parameters and fragment: ``scheme://netloc/path``."""
        url = self.scheme + self.NETLOC_SEPARATOR
        if self.netloc:
            url += self.netloc
        url += quote(str(self.path), safe=self.NETLOC_SEPARATOR[:-1])
        return url

    @classmethod
    def __get_servers_and_netloc(cls, netloc: str, scheme: str) -> tuple[str, list[str] | None]:
        """Parse list of servers.

        :param netloc: Netloc object
        :param scheme: http or https
        :returns: netloc of the first node and the uris of the other nodes, or ``None`` for a single server
            or a scheme that has no clusters
        """
        # If scheme is not http or https, it cannot work in cluster mode
        if scheme not in ["http", "https"] or cls.SERVERS_DIVIDER not in netloc:
            # So servers will be None
            return netloc, None

        # Otherwise parse servers
        servers = netloc.split(cls.SERVERS_DIVIDER)
        node_with_possible_auth = servers[0]
        if cls.SERVERS_SEPARATOR in node_with_possible_auth:
            auth, _ = node_with_possible_auth.split(cls.SERVERS_SEPARATOR)
            return (
                node_with_possible_auth,
                [f"{scheme}{cls.NETLOC_SEPARATOR}{auth}{cls.SERVERS_SEPARATOR}{host}" for host in servers[1:]],
            )
        return (
            node_with_possible_auth,
            [f"{scheme}{cls.NETLOC_SEPARATOR}{host}" for host in servers[1:]],
        )

    @classmethod
    def __parse(cls, uri: str) -> "Uri":
        """Parse uri from string.

        :param uri: : scheme://username:password@host:port/path;parameters?query
        :returns: parsed ``Uri``
        """
        result = urlparse(uri)
        query = parse_qs(result.query)
        netloc, servers = cls.__get_servers_and_netloc(result.netloc, result.scheme)
        return Uri(result.scheme, netloc, result.path, result.params, query, result.fragment, servers)

    @classmethod
    def validate(cls, uri: str) -> None:
        """Validate uri from string.

        :param uri: : scheme://username:password@host:port/path;parameters?query
        :raises DekerValidationError: if the uri is not a non-empty string, or the path of a ``file://`` uri
          is invalid
        """
        if not isinstance(uri, str):
            raise DekerValidationError(f"Invalid uri type: {uri} - {type(uri)}")
        if not uri or uri.isspace():
            raise DekerValidationError("Empty uri passed")

        parsed_uri = cls.__parse(uri)
        if parsed_uri.scheme == "file":
            pathname = Path(parsed_uri.path)
            try:
                is_path_valid(pathname)
            except Exception as e:
                raise DekerValidationError(f"Invalid uri: {e}; {uri}")

    @classmethod
    def create(cls, uri: str) -> "Uri":
        """Create parsed and validated uri from string.

        :param uri: scheme://username:password@host:port/path;parameters?query
        :returns: parsed and validated ``Uri``
        :raises DekerValidationError: if the uri is not a non-empty string, or the path of a ``file://`` uri
          is invalid
        """
        cls.validate(uri)
        return cls.__parse(uri)

    def __truediv__(self, other: str | Path) -> "Uri":
        """Join uri to Path or string.

        :param other: Path or string to join
        :returns: ``Uri`` with the joined path
        """
        other = str(other)
        path = self.PATH_SEPARATOR.join((self.path, other.strip()))
        netloc, servers = self.__get_servers_and_netloc(self.netloc, self.scheme)
        return Uri(self.scheme, netloc, path, self.params, self.query, self.fragment, servers)

    def __itruediv__(self, other: str | Path) -> "Uri":
        """Join uri to Path or string in place: the same as ``/``, a ``Uri`` is immutable.

        :param other: Path or string to join
        :returns: ``Uri`` with the joined path
        """
        return self.__truediv__(other)
