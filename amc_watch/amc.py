"""Fail-closed AMC GraphQL transport and bounded batch query builders."""

from __future__ import annotations

import base64
import gzip
import http.client
import json
import socket
import urllib.error
import urllib.request
from urllib.parse import unquote
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from .domain import BOOK_URL, fixed_offset, normalize_status


API_URL = "https://graph.amctheatres.com/"
AMC_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "x-amc-device-id": "54b8e86c-f66a-49d9-a6a0-53c536db96c2",
    "x-amc-device-os-type": "Android",
    "x-amc-device-os-version": "13",
    "x-amc-device-app-version": "7.0.137",
    "User-Agent": "AMCTheatres/7.0.137 (Android)",
}

STATUS_FRAGMENT = """fragment statusShowtime on Showtime{id status isSoldOut isAlmostSoldOut}"""
SEAT_FRAGMENT = """fragment viewSeatsShowtime on Showtime{id seatingLayout{
 rows columns seats{id row column available seatTier shouldDisplay type name}}}"""
DISCOVERY_FRAGMENT = """items{movie{name movieId slug} theatres{formats{items{
 attributes{code name} groups(first:15){edges{node{
 format:showtimeGroupHeadingAttribute{code name}
 showtimes(first:100,filter:{excludeStatus:[PAST_SELL_DATE]}){edges{node{
 showtimeId showDateTimeUtc utcOffset status isSoldOut isAlmostSoldOut}}}}}}}}}}"""
MOVIE_SEARCH_QUERY = """query monitorMovieSearch($query:String!){viewer{
 search(query:$query,types:all,first:12){edges{node{title type movieId movie{
 movieId name slug releaseDateUtc}}}}}}"""
LOCATION_QUERY = """query monitorLocation($query:String!){viewer{
 location(query:$query,first:3){edges{node{id title latitude longitude theatres(first:20){
 edges{node{theatreId slug name longName addressLine1 city stateCode postalCode distance
 timezoneAbbreviation utcOffset ticketable}}}}}}}}"""
# Global, ZIP-independent catalogs used to pre-populate the static reference data
# the setup wizard reads from (theatres + presentation formats). Both are Relay
# connections, paginated with $after until pageInfo.hasNextPage is false.
THEATRES_CATALOG_QUERY = """query monitorTheatres($first:Int!,$after:String){viewer{
 theatres(first:$first,after:$after){count pageInfo{hasNextPage endCursor}
 edges{node{theatreId slug name longName addressLine1 city stateCode postalCode
 latitude longitude timezoneAbbreviation utcOffset brand marketSlug marketName ticketable}}}}}"""
FORMATS_CATALOG_QUERY = """query monitorFormats($first:Int!,$after:String){viewer{
 attributes(groups:[FORMAT],type:ALL,first:$first,after:$after){count
 pageInfo{hasNextPage endCursor} edges{node{code name abbreviation sort}}}}}"""


class AMCError(RuntimeError):
    """Base class whose message is safe to put in logs."""


class ProxyEndpointError(AMCError):
    """The configured proxy endpoint is unavailable or rejected the connection."""


class AMCUpstreamCooldown(AMCError):
    """AMC requested that all traffic stop for a bounded period."""

    def __init__(self, status_code: int, seconds: int):
        self.status_code = status_code
        self.seconds = max(int(seconds), 60)
        super().__init__(f"AMC returned HTTP {status_code}; global cooldown required")


class GraphQLResponseError(AMCError):
    pass


class _FailClosedProxyHandler(urllib.request.ProxyHandler):
    """Proxy handler that never honors process-level NO_PROXY bypasses."""

    def proxy_open(
        self, request: urllib.request.Request, proxy: str, proxy_type: str
    ) -> Any:
        original_type = request.type
        parsed_type, user, password, hostport = urllib.request._parse_proxy(proxy)
        if parsed_type is None:
            parsed_type = original_type
        if user and password:
            user_pass = f"{unquote(user)}:{unquote(password)}"
            encoded = base64.b64encode(user_pass.encode()).decode("ascii")
            request.add_header("Proxy-authorization", f"Basic {encoded}")
        hostport = unquote(hostport)
        request.set_proxy(hostport, parsed_type)
        if original_type == parsed_type or original_type == "https":
            return None
        return self.parent.open(request, timeout=request.timeout)


def _validate_proxy(proxy_url: str) -> None:
    if not proxy_url.startswith(("http://", "https://")):
        raise ValueError("AMC proxy must be an HTTP(S) URL")
    if "@" not in proxy_url:
        raise ValueError("AMC proxy must include credentials")


class GraphQLClient:
    """One-attempt transport; retries, cooldown, and failover belong to the worker."""

    def __init__(self, proxy_url: str, *, timeout_seconds: float = 20):
        if not proxy_url:
            raise RuntimeError("AMC proxy is required; refusing direct AMC access")
        _validate_proxy(proxy_url)
        self.timeout_seconds = timeout_seconds
        self._proxy_url = proxy_url
        self._opener = self._build_opener(proxy_url)

    @staticmethod
    def _build_opener(proxy_url: str) -> urllib.request.OpenerDirector:
        return urllib.request.build_opener(
            _FailClosedProxyHandler({"http": proxy_url, "https": proxy_url})
        )

    def use_proxy(self, proxy_url: str) -> None:
        """Select a health-checked backup without touching the global request gate."""
        _validate_proxy(proxy_url)
        self._opener = self._build_opener(proxy_url)
        self._proxy_url = proxy_url

    def query(self, query: str, variables: Mapping[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            API_URL,
            data=json.dumps({"query": query, "variables": dict(variables)}).encode(),
            method="POST",
            headers=AMC_HEADERS,
        )
        try:
            with self._opener.open(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
                if response.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                payload = json.loads(raw)
        except urllib.error.HTTPError as exc:
            if exc.code == 407:
                raise ProxyEndpointError("proxy authentication failed") from exc
            if exc.code in {403, 429, 503}:
                try:
                    seconds = int(float(exc.headers.get("Retry-After") or 3600))
                except (TypeError, ValueError):
                    seconds = 3600
                # Cap the cooldown: a hostile/broken Retry-After (or an HTTP-date
                # we can't parse) must never strand the worker in a cooldown it
                # can only leave after a successful request it can no longer make.
                seconds = min(max(seconds, 0), 3600)
                raise AMCUpstreamCooldown(exc.code, seconds) from exc
            raise AMCError(f"AMC returned HTTP {exc.code}") from exc
        except (
            urllib.error.URLError,
            TimeoutError,
            ConnectionError,
            socket.timeout,
            http.client.RemoteDisconnected,
        ) as exc:
            raise ProxyEndpointError("proxy connection interrupted") from exc
        except (json.JSONDecodeError, gzip.BadGzipFile) as exc:
            raise GraphQLResponseError("AMC returned an invalid response") from exc

        errors = payload.get("errors") or []
        if errors:
            # GraphQL error text may contain upstream internals, so do not echo it.
            raise GraphQLResponseError("AMC GraphQL request was rejected")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise GraphQLResponseError("AMC GraphQL response did not contain data")
        return data


@dataclass(frozen=True)
class ProxyPool:
    endpoints: tuple[str, ...]
    active_index: int = 0
    attempted_indices: tuple[int, ...] = ()

    @classmethod
    def from_values(cls, primary: str, backups: Sequence[str]) -> "ProxyPool":
        values: list[str] = []
        for endpoint in (primary, *backups):
            if endpoint and endpoint not in values:
                _validate_proxy(endpoint)
                values.append(endpoint)
        if not values:
            raise RuntimeError("at least one AMC proxy endpoint is required")
        return cls(tuple(values))

    @property
    def active(self) -> str:
        return self.endpoints[self.active_index]

    def failover(self) -> "ProxyPool | None":
        attempted = set(self.attempted_indices) | {self.active_index}
        for index in range(len(self.endpoints)):
            if index not in attempted:
                return ProxyPool(self.endpoints, index, tuple(sorted(attempted)))
        return None

    def recovered(self) -> "ProxyPool":
        return ProxyPool(self.endpoints, self.active_index, ())


def _aliased_showtime_query(
    showtime_ids: Iterable[str], *, prefix: str, fragment_name: str, fragment: str
) -> tuple[str, dict[str, int], dict[str, str]]:
    ids = [str(int(value)) for value in showtime_ids]
    if not ids:
        raise ValueError("at least one showtime is required")
    if len(ids) > 8:
        raise ValueError("showtime batches are limited to eight")
    definitions = ",".join(f"$showtime{index}:Int!" for index in range(len(ids)))
    selections = " ".join(
        f"{prefix}{index}:showtime(id:$showtime{index}){{...{fragment_name}}}"
        for index in range(len(ids))
    )
    variables = {f"showtime{index}": int(value) for index, value in enumerate(ids)}
    aliases = {f"{prefix}{index}": value for index, value in enumerate(ids)}
    return (
        f"query {prefix}Batch({definitions}){{viewer{{{selections}}}}} {fragment}",
        variables,
        aliases,
    )


def status_batch_query(showtime_ids: Iterable[str]) -> tuple[str, dict[str, int], dict[str, str]]:
    return _aliased_showtime_query(
        showtime_ids,
        prefix="status",
        fragment_name="statusShowtime",
        fragment=STATUS_FRAGMENT,
    )


def seatmap_batch_query(showtime_ids: Iterable[str]) -> tuple[str, dict[str, int], dict[str, str]]:
    return _aliased_showtime_query(
        showtime_ids,
        prefix="seat",
        fragment_name="viewSeatsShowtime",
        fragment=SEAT_FRAGMENT,
    )


def selectable_dates_batch_query(
    movies: Iterable[Mapping[str, Any]],
) -> tuple[str, dict[str, str], dict[str, Mapping[str, Any]]]:
    values = [movie for movie in movies if movie.get("slug")]
    if not values or len(values) > 8:
        raise ValueError("selectable-date batches require one to eight movies")
    definitions = ",".join(f"$movie{index}:String!" for index in range(len(values)))
    selections: list[str] = []
    variables: dict[str, str] = {}
    aliases: dict[str, Mapping[str, Any]] = {}
    for index, movie in enumerate(values):
        alias = f"movieDates{index}"
        selections.append(f"{alias}:selectableDates(movieSlug:$movie{index}){{dates}}")
        variables[f"movie{index}"] = str(movie["slug"])
        aliases[alias] = movie
    return (
        f"query monitorMovieDatesBatch({definitions}){{viewer{{{' '.join(selections)}}}}}",
        variables,
        aliases,
    )


def discovery_batch_query(
    targets: Iterable[Mapping[str, str]],
) -> tuple[str, dict[str, str], dict[str, Mapping[str, str]]]:
    values = list(targets)
    if not values or len(values) > 4:
        raise ValueError("discovery batches require one to four theatre/date targets")
    definitions: list[str] = []
    selections: list[str] = []
    variables: dict[str, str] = {}
    aliases: dict[str, Mapping[str, str]] = {}
    for index, target in enumerate(values):
        definitions.extend((f"$slug{index}:String!", f"$date{index}:Date!"))
        alias = f"discovery{index}"
        selections.append(
            f"{alias}:movies(theatreSlug:$slug{index},date:$date{index})"
            f"{{{DISCOVERY_FRAGMENT}}}"
        )
        variables[f"slug{index}"] = target["theatre_slug"]
        variables[f"date{index}"] = target["date"]
        aliases[alias] = target
    return (
        f"query monitorDiscoveryBatch({','.join(definitions)})"
        f"{{viewer{{user{{{' '.join(selections)}}}}}}}",
        variables,
        aliases,
    )


def parse_selectable_dates(
    viewer: Mapping[str, Any], aliases: Mapping[str, Mapping[str, Any]]
) -> dict[str, set[date]]:
    result: dict[str, set[date]] = {}
    for alias, movie in aliases.items():
        slug = str(movie["slug"])
        # Parse the movie's window bounds once and defensively: malformed
        # metadata (e.g. "8/1/2026") must not raise and poison the whole batch
        # into an infinite retry that burns AMC request slots.
        def _bound(key: str) -> date | None:
            value = movie.get(key)
            if not value:
                return None
            try:
                return date.fromisoformat(str(value)[:10])
            except ValueError:
                return None

        lower, upper = _bound("not_before"), _bound("not_after")
        values: set[date] = set()
        for raw in ((viewer.get(alias) or {}).get("dates") or []):
            try:
                selected = date.fromisoformat(str(raw)[:10])
            except ValueError:
                continue
            if lower and selected < lower:
                continue
            if upper and selected > upper:
                continue
            values.add(selected)
        result[slug] = values
    return result


def parse_discovery_payload(
    payload: Mapping[str, Any], *, theatre_slug: str
) -> list[dict[str, Any]]:
    """Normalize AMC discovery while retaining the lightweight status fields."""
    found: dict[str, dict[str, Any]] = {}
    for item in payload.get("items") or []:
        movie = item.get("movie") or {}
        if "event" in str(movie.get("name") or "").casefold():
            continue
        for theatre in item.get("theatres") or []:
            for value in ((theatre.get("formats") or {}).get("items") or []):
                attributes = {
                    str(attribute.get("code") or "").casefold()
                    for attribute in value.get("attributes") or []
                }
                for edge in ((value.get("groups") or {}).get("edges") or []):
                    node = edge.get("node") or {}
                    group_format = node.get("format") or {}
                    format_code = str(group_format.get("code") or "").casefold()
                    for show_edge in ((node.get("showtimes") or {}).get("edges") or []):
                        remote = show_edge.get("node") or {}
                        when_raw = remote.get("showDateTimeUtc")
                        try:
                            when_utc = datetime.fromisoformat(str(when_raw).replace("Z", "+00:00"))
                        except (TypeError, ValueError):
                            continue
                        if when_utc <= datetime.now(timezone.utc):
                            continue
                        showtime_id = str(remote.get("showtimeId") or "")
                        if not showtime_id:
                            continue
                        local = when_utc.astimezone(fixed_offset(remote.get("utcOffset")))
                        found[showtime_id] = {
                            "showtime_id": showtime_id,
                            "movie_id": movie.get("movieId"),
                            "movie_slug": movie.get("slug"),
                            "movie_name": str(movie.get("name") or "Unknown movie"),
                            "theatre_slug": theatre_slug,
                            "format_code": format_code,
                            "format_name": str(group_format.get("name") or format_code or "AMC"),
                            "attribute_codes": sorted(attributes | {format_code}),
                            "showtime_at": when_utc,
                            "utc_offset": remote.get("utcOffset"),
                            "showtime_local": local,
                            "status": normalize_status(remote.get("status")) or None,
                            "is_sold_out": bool(remote.get("isSoldOut")),
                            "is_almost_sold_out": bool(remote.get("isAlmostSoldOut")),
                            "book_url": BOOK_URL.format(showtime_id=showtime_id),
                        }
    return list(found.values())


def parse_location_catalog(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    theatres: dict[str, dict[str, Any]] = {}
    viewer = data.get("viewer") or {}
    for location_edge in ((viewer.get("location") or {}).get("edges") or []):
        location = location_edge.get("node") or {}
        for theatre_edge in ((location.get("theatres") or {}).get("edges") or []):
            theatre = theatre_edge.get("node") or {}
            slug = str(theatre.get("slug") or "")
            if not slug or theatre.get("ticketable") is False:
                continue
            theatres[slug] = {
                key: theatre.get(key)
                for key in (
                    "theatreId",
                    "slug",
                    "name",
                    "longName",
                    "addressLine1",
                    "city",
                    "stateCode",
                    "postalCode",
                    "distance",
                    "timezoneAbbreviation",
                    "utcOffset",
                )
            }
    return sorted(theatres.values(), key=lambda theatre: float(theatre.get("distance") or 9999))


def parse_movie_catalog(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    movies: dict[str, dict[str, Any]] = {}
    viewer = data.get("viewer") or {}
    for edge in ((viewer.get("search") or {}).get("edges") or []):
        node = edge.get("node") or {}
        movie = node.get("movie") or {}
        movie_id = movie.get("movieId") or node.get("movieId")
        name = movie.get("name") or node.get("title")
        if not movie_id or not name or "event" in str(name).casefold():
            continue
        movies[str(movie_id)] = {
            "movie_id": int(movie_id),
            "name": str(name),
            "slug": movie.get("slug"),
            "release_date": movie.get("releaseDateUtc"),
        }
    return list(movies.values())


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _page_info(connection: Mapping[str, Any]) -> dict[str, Any]:
    page = connection.get("pageInfo") or {}
    return {
        "count": connection.get("count"),
        "has_next_page": bool(page.get("hasNextPage")),
        "end_cursor": page.get("endCursor"),
    }


def parse_theatres_catalog(data: Mapping[str, Any]) -> dict[str, Any]:
    """Parse one page of the global ``viewer.theatres`` connection.

    Returns ``{theatres: [...], count, has_next_page, end_cursor}``. Theatre dicts
    keep AMC's raw camelCase keys so the persistence layer can share the logic
    that already consumes :func:`parse_location_catalog` output.
    """

    connection = ((data.get("viewer") or {}).get("theatres")) or {}
    theatres: dict[str, dict[str, Any]] = {}
    for edge in connection.get("edges") or []:
        node = edge.get("node") or {}
        slug = str(node.get("slug") or "")
        if not slug or node.get("ticketable") is False:
            continue
        theatres[slug] = {
            "theatreId": node.get("theatreId"),
            "slug": slug,
            "name": node.get("name"),
            "longName": node.get("longName"),
            "addressLine1": node.get("addressLine1"),
            "city": node.get("city"),
            "stateCode": node.get("stateCode"),
            "postalCode": node.get("postalCode"),
            "latitude": _as_float(node.get("latitude")),
            "longitude": _as_float(node.get("longitude")),
            "timezoneAbbreviation": node.get("timezoneAbbreviation"),
            "utcOffset": node.get("utcOffset"),
            "brand": node.get("brand"),
            "marketSlug": node.get("marketSlug"),
            "marketName": node.get("marketName"),
        }
    return {"theatres": list(theatres.values()), **_page_info(connection)}


def parse_formats_catalog(data: Mapping[str, Any]) -> dict[str, Any]:
    """Parse one page of the global ``viewer.attributes(groups:[FORMAT])`` connection."""

    connection = ((data.get("viewer") or {}).get("attributes")) or {}
    formats: dict[str, dict[str, Any]] = {}
    for edge in connection.get("edges") or []:
        node = edge.get("node") or {}
        code = str(node.get("code") or "").strip().casefold()
        name = str(node.get("name") or "").strip()
        if not code or not name:
            continue
        formats[code] = {
            "code": code,
            "name": name,
            "abbreviation": node.get("abbreviation"),
            "sort": node.get("sort"),
        }
    return {"formats": list(formats.values()), **_page_info(connection)}


def parse_location_centroid(data: Mapping[str, Any]) -> dict[str, float] | None:
    """Extract the query's own lat/long from a ``viewer.location`` response.

    Used to geocode a user ZIP once and cache it, so nearest-theatre ranking runs
    entirely against the static catalog thereafter.
    """

    for edge in (((data.get("viewer") or {}).get("location") or {}).get("edges") or []):
        node = edge.get("node") or {}
        latitude = _as_float(node.get("latitude"))
        longitude = _as_float(node.get("longitude"))
        if latitude is not None and longitude is not None:
            return {"latitude": latitude, "longitude": longitude}
    return None
