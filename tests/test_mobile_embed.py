import unittest

import discord

from amc_watch.discord_embed import (
    AMC_HOME_URL,
    BookingLinkView,
    build_alert_embed,
    rank_adjacent_runs,
    render_mobile_seat_map,
    safe_booking_url,
)
from amc_watch.discord_models import Seat, SeatPreset, UserAlert


def layout(open_coordinates=(), rows=12, columns=42):
    open_set = set(open_coordinates)
    labels = "ABCDEFGHJKLM"
    return tuple(
        Seat(
            row=row,
            column=column,
            name=f"{labels[row]}{column + 1}",
            available=(row, column) in open_set,
        )
        for row in range(rows)
        for column in range(columns)
    )


class MobileSeatEmbedTests(unittest.IsolatedAsyncioTestCase):
    def test_rank_supports_one_to_six_adjacent_seats(self):
        seats = layout({(8, column) for column in range(18, 24)})
        for count in range(1, 7):
            runs = rank_adjacent_runs(seats, count, SeatPreset.CENTER_BACK)
            self.assertTrue(runs, count)
            self.assertEqual(len(runs[0].names), count)
            self.assertGreaterEqual(runs[0].score, 90)

    def test_rank_excludes_front_back_edges_and_nonstandard_seats(self):
        available = {(0, 20), (0, 21), (11, 20), (11, 21), (8, 0), (8, 1)}
        seats = list(layout(available))
        self.assertEqual(rank_adjacent_runs(seats, 2, SeatPreset.CENTER_BACK), [])
        seats = list(layout({(8, 20), (8, 21)}))
        seat = seats[8 * 42 + 21]
        seats[8 * 42 + 21] = Seat(
            row=seat.row,
            column=seat.column,
            name=seat.name,
            available=True,
            type="Companion",
        )
        self.assertEqual(rank_adjacent_runs(seats, 2, SeatPreset.CENTER_BACK), [])

    def test_mobile_map_hides_unused_rows_and_edges(self):
        seats = layout({(8, 20), (8, 21)})
        text = render_mobile_seat_map(
            seats, {(8, 20), (8, 21)}, SeatPreset.CENTER_BACK
        )
        lines = text.splitlines()
        row_labels = [line.split()[0] for line in lines[1:]]
        self.assertNotIn("A", row_labels)
        self.assertNotIn("B", row_labels)
        self.assertNotIn("M", row_labels)
        self.assertIn("J", row_labels)
        self.assertIn("◆◆", text)
        self.assertTrue(all(len(line) <= 26 for line in lines))

    def test_preset_changes_visible_rows(self):
        seats = layout()
        front = render_mobile_seat_map(seats, (), SeatPreset.CENTER_FRONT)
        back = render_mobile_seat_map(seats, (), SeatPreset.CENTER_BACK)
        self.assertIn(" C ", front)
        self.assertNotIn(" K ", front)
        self.assertNotIn(" C ", back)
        self.assertIn(" K ", back)

    async def test_alert_embed_is_compact_and_has_link_button(self):
        seats = layout({(8, 20), (8, 21)})
        recommendations = tuple(
            rank_adjacent_runs(seats, 2, SeatPreset.CENTER_BACK)[:3]
        )
        alert = UserAlert(
            outbox_id="out-1",
            guild_id=1,
            channel_id=2,
            movie="Example Feature",
            theatre="AMC Example 8",
            when_local="Sat, Jul 18 · 7:00 PM",
            format_name="IMAX 70MM",
            adjacent_seats=2,
            preset=SeatPreset.CENTER_BACK,
            booking_url="https://www.amctheatres.com/showtimes/123",
            seats=seats,
            recommendations=recommendations,
        )
        embed = build_alert_embed(alert)
        self.assertLessEqual(len(embed.description or ""), 4096)
        self.assertIn("Front/back rows and far edges are hidden", embed.description)
        self.assertNotIn("@everyone", embed.description)
        view = BookingLinkView(alert.booking_url)
        self.assertEqual(view.children[0].url, alert.booking_url)
        self.assertEqual(view.children[0].label, "Open AMC")

    def test_only_https_amc_links_are_clickable(self):
        self.assertEqual(safe_booking_url("javascript:alert(1)"), AMC_HOME_URL)
        self.assertEqual(safe_booking_url("https://evil.example/amc"), AMC_HOME_URL)
        good = "https://www.amctheatres.com/movies/example-feature"
        self.assertEqual(safe_booking_url(good), good)


if __name__ == "__main__":
    unittest.main()
