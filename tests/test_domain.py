from __future__ import annotations

import unittest

from amc_watch.domain import (
    adaptive_status_interval,
    compact_seats,
    mobile_seatmap,
    rank_runs,
)


def layout(open_coordinates: set[tuple[int, int]], *, rows: int = 10, columns: int = 31):
    return [
        {
            "name": f"{chr(65 + row)}{column + 1}",
            "row": row,
            "column": column,
            "available": (row, column) in open_coordinates,
            "type": "CanReserve",
            "shouldDisplay": True,
        }
        for row in range(rows)
        for column in range(columns)
    ]


class DomainTests(unittest.TestCase):
    def test_ranks_one_to_six_adjacent_seats(self):
        seats = layout({(6, column) for column in range(12, 18)})
        for count in range(1, 7):
            runs = rank_runs(seats, count=count, preset="center-back")
            self.assertTrue(runs)
            self.assertEqual(len(runs[0].names), count)
            self.assertLessEqual(runs[0].center_offset, 0.30)

    def test_front_back_and_edges_are_not_eligible(self):
        seats = layout({(0, 14), (0, 15), (9, 14), (9, 15), (6, 0), (6, 1)})
        self.assertEqual(rank_runs(seats, count=2, preset="center-back"), [])

    def test_mobile_map_only_contains_preferred_center_window(self):
        seats = layout({(row, column) for row in range(10) for column in range(31)})
        compact = compact_seats(seats, {(6, 15), (6, 16)})
        rendered = mobile_seatmap(compact, preset="center-back")
        lines = rendered.splitlines()
        self.assertLessEqual(max(len(line) for line in lines), 32)
        self.assertNotIn("A  ", rendered)
        self.assertNotIn("J  ", rendered)
        self.assertIn("◆◆", rendered)

    def test_adaptive_cadence_reserves_half_the_request_lane(self):
        self.assertEqual(adaptive_status_interval(8), 15)
        self.assertEqual(adaptive_status_interval(40), 30)
        self.assertEqual(adaptive_status_interval(80), 60)
        self.assertEqual(adaptive_status_interval(8, degraded=True), 30)


if __name__ == "__main__":
    unittest.main()
