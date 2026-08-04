# AMC GraphQL map

Endpoint: `https://graph.amctheatres.com/`

The monitor uses the anonymous read-only viewer surface exposed by AMC's mobile
application. No customer login, checkout token, payment data, or account cookie
is used. The full live introspection result is checked in as
`amc-graphql-introspection.json`; regenerate it with:

```bash
uv run python tools/dump_schema.py
```

## Operations used by the monitor

### Showtime discovery

```graphql
query ($slug: String!, $date: Date!) {
  viewer {
    user {
      movies(theatreSlug: $slug, date: $date) {
        items {
          movie { name movieId }
          theatres {
            formats {
              items {
                attributes { code name }
                groups(first: 15) {
                  edges {
                    node {
                      format: showtimeGroupHeadingAttribute { code name }
                      showtimes(
                        first: 100
                        filter: { excludeStatus: [PAST_SELL_DATE] }
                      ) {
                        edges {
                          node {
                            showtimeId
                            showDateTimeUtc
                            utcOffset
                            status
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

Filtering by theatre, movie, presentation format, and viewing window happens
locally after discovery.

### Seat map

```graphql
query viewSeats($showtimeId: Int!) {
  viewer {
    showtime(id: $showtimeId) {
      id
      seatingLayout {
        rows
        columns
        seats {
          id
          row
          column
          available
          seatTier
          shouldDisplay
          type
          name
        }
      }
    }
  }
}
```

This is the app's lightweight read-only `viewSeats` shape, rather than its much
larger checkout `seatingLayout` operation. Only visible seats whose `type` is
exactly `CanReserve` are eligible. Wheelchair, companion, aisle, hidden, and
non-seat cells are preserved for rendering but never count toward availability.

## Auditorium geometry

The API includes aisle and non-seat cells in its grid. Preserving those cells
lets alert images reflect the returned auditorium shape instead of drawing a
generic rectangle.

## Transport behavior

- `POST` JSON GraphQL requests
- gzip responses accepted
- Android AMC app identification headers
- one serialized request at a time
- bounded aliases group up to eight independent `viewSeats` selections
- 30-second minimum inter-request gap
- hard cooldown on 403, 407, 429, or 503
- no automatic retries within a request

Proxy and Discord credentials are runtime configuration and must never appear
in the schema, application logs, or checked-in files.
