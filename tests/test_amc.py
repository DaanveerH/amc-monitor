from __future__ import annotations

import unittest

from amc_watch.amc import (
    parse_formats_catalog,
    parse_location_centroid,
    parse_theatres_catalog,
)


def _theatre_node(**overrides):
    node = {
        "theatreId": 1234,
        "slug": "amc-example-8",
        "name": "AMC Example 8",
        "longName": "AMC Example 8",
        "addressLine1": "1998 Broadway",
        "city": "New York",
        "stateCode": "NY",
        "postalCode": "00000",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezoneAbbreviation": "EDT",
        "utcOffset": "-04:00",
        "brand": "AMC",
        "marketSlug": "new-york",
        "marketName": "New York",
        "ticketable": True,
    }
    node.update(overrides)
    return {"node": node}


class TheatresCatalogParserTests(unittest.TestCase):
    def test_parses_page_and_pagination(self):
        data = {
            "viewer": {
                "theatres": {
                    "count": 900,
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor-2"},
                    "edges": [_theatre_node()],
                }
            }
        }
        result = parse_theatres_catalog(data)
        self.assertEqual(result["count"], 900)
        self.assertTrue(result["has_next_page"])
        self.assertEqual(result["end_cursor"], "cursor-2")
        self.assertEqual(len(result["theatres"]), 1)
        theatre = result["theatres"][0]
        self.assertEqual(theatre["slug"], "amc-example-8")
        # Numeric lat/long, not strings.
        self.assertIsInstance(theatre["latitude"], float)
        self.assertAlmostEqual(theatre["longitude"], -118.2437)

    def test_drops_non_ticketable_and_slugless_and_dedupes(self):
        data = {
            "viewer": {
                "theatres": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "edges": [
                        _theatre_node(),
                        _theatre_node(),  # duplicate slug -> deduped
                        _theatre_node(slug="closed", ticketable=False),
                        _theatre_node(slug=""),
                    ],
                }
            }
        }
        result = parse_theatres_catalog(data)
        self.assertEqual([t["slug"] for t in result["theatres"]], ["amc-example-8"])
        self.assertFalse(result["has_next_page"])

    def test_tolerates_missing_coordinates(self):
        data = {
            "viewer": {
                "theatres": {
                    "pageInfo": {},
                    "edges": [_theatre_node(latitude=None, longitude="")],
                }
            }
        }
        theatre = parse_theatres_catalog(data)["theatres"][0]
        self.assertIsNone(theatre["latitude"])
        self.assertIsNone(theatre["longitude"])


class FormatsCatalogParserTests(unittest.TestCase):
    def test_casefolds_codes_and_dedupes(self):
        data = {
            "viewer": {
                "attributes": {
                    "count": 2,
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "edges": [
                        {"node": {"code": "IMAX70MM", "name": "IMAX 70MM", "sort": 1}},
                        {"node": {"code": "imax70mm", "name": "IMAX 70MM", "sort": 1}},
                        {"node": {"code": "DolbyCinemaAtAMCPrime", "name": "Dolby Cinema at AMC"}},
                        {"node": {"code": "", "name": "junk"}},
                    ],
                }
            }
        }
        result = parse_formats_catalog(data)
        codes = {f["code"] for f in result["formats"]}
        self.assertEqual(codes, {"imax70mm", "dolbycinemaatamcprime"})


class LocationCentroidParserTests(unittest.TestCase):
    def test_extracts_first_valid_centroid(self):
        data = {
            "viewer": {
                "location": {
                    "edges": [
                        {"node": {"latitude": None, "longitude": -73.0}},
                        {"node": {"latitude": 34.05, "longitude": -118.24}},
                    ]
                }
            }
        }
        centroid = parse_location_centroid(data)
        self.assertEqual(centroid, {"latitude": 34.05, "longitude": -118.24})

    def test_returns_none_when_absent(self):
        self.assertIsNone(parse_location_centroid({"viewer": {"location": {"edges": []}}}))


if __name__ == "__main__":
    unittest.main()
