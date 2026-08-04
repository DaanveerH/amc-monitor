# AMC format / attribute codes

Reference catalog of AMC presentation-format and attribute codes. The monitor
matches these against the
GraphQL `showtimeGroupHeadingAttribute.code` and each format's `attributes[].code`,
**casefolded on both sides** — so only the letters/digits matter, not the case
or how a source happens to capitalize them.

## Authoritative source: what the monitor observes

The public web does **not** publish a guaranteed-complete or exactly-cased list
of the codes returned by the GraphQL API the monitor calls
(`graph.amctheatres.com`). AMC's own docs and third-party mirrors confirm the
only authoritative list is a live showtimes response.

The database catalog records every `code -> name` pair observed during
discovery. That live catalog is the ground truth for a particular theatre.

## Confirmed codes (from AMC docs / live data)

Presentation formats:

| code (casefolded) | name |
| --- | --- |
| `imax` | IMAX at AMC |
| `imax70mm` | IMAX 70mm |
| `dolbycinemaatamcprime` | Dolby Cinema at AMC |
| `laseratamc` | Laser at AMC (PLF) |
| `reald3d` | RealD 3D |
| `digitalprojection` | Digital (standard) |

Amenities / non-presentation attributes (not usually `format_codes` targets):
`reclinerseating`, `dineinseatsideservice`, `cinemasuites`, `macguffins`,
`dinein`, `amcstubsalist`, `sensoryfriendly`, `madetoorderbeverages`,
`stadiumseating`.

## Inferred codes (AMC premium formats — verify against `observed_formats`)

These are AMC's known premium formats whose exact code strings were **not**
confirmable from public sources. Treat as best-guess until seen in
`schedule.observed_formats`:

| likely code | name |
| --- | --- |
| `imaxwithlaseratamc` | IMAX with Laser at AMC |
| `dolbyatmos` | Dolby Atmos |
| `bigd` | BigD |
| `dbox` | D-BOX |
| `xl` / `xlatamc` | XL at AMC |
| `opencaption` | Open Caption |
| `70mm` | **Standard 70mm film — code UNCONFIRMED** |

### Standard-70mm caveat

`70mm` is a best guess; the true code could differ. Verify it against a live
catalog before using it as a monitor filter.

## Sources

- [AMC Developer Portal — Attributes](https://developers.amctheatres.com/Attributes) (403s to automated fetch; requires a vendor key)
- [AMC Developer Portal — Showtimes](https://developers.amctheatres.com/Showtimes)
- [Parse.bot — AMC Theatres API summary](https://parse.bot/marketplace/806399d8-6960-4d3e-9ea0-da32b3129d63/amctheatres-com-api) (states the complete list requires inspecting a live `format` field)
</content>
</invoke>
