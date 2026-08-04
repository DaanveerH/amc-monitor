# AMC Android network findings

Static analysis date: 2026-07-18

## Artifact and provenance

- Package: `com.amc`
- Release: `7.0.137` (`602147`), published 2026-07-15
- Local bundle: `apk/amc-7.0.137.apkm`
- Bundle SHA-256: `d2846970e4039ed527a6a2a0510390ec30eaefd25b8debba68d40251e7e370ac`
- Base APK SHA-256: `b7a1935095abf6a669177a1b56fe49660a224728a06705b79f831a747b44ff42`
- ARM64 Flutter `libapp.so` SHA-256: `87fbed66b87314218488c926f94f30a4fdecfcbc01db21ac745d65db58b63289`

The downloaded ARM64 split was locally checked with `keytool`. Its signer
certificate SHA-256 is
`A3:85:95:CB:9F:EF:41:D2:89:7C:C5:B6:C3:CC:EB:00:E8:97:5B:C6:7D:09:2E:28:99:2D:51:58:8D:07:7C:D1`,
matching the certificate published for AMC's APKMirror release.

The bundle is intentionally gitignored because it is a 52 MB third-party binary.

## Network surface

The current Android release is a Flutter application. Its compiled native
snapshot contains one AMC GraphQL origin:

```text
https://graph.amctheatres.com
```

No second AMC availability or seat-map API origin was present. The other AMC
URLs in the snapshot are the public website, merchandise site, and Cloudinary
assets. The browser seat picker also resolves the seat flow through AMC's own
application layer, so there is no evidence that switching to an undocumented
alternate host would improve reliability.

The release sends these app-identification headers:

```text
x-amc-device-id
x-amc-device-os-type
x-amc-device-os-version
x-amc-device-app-version
```

It also contains `x-amc-account-id`, which applies to authenticated account
flows and is not needed by this anonymous monitor.

## Relevant embedded operations

The app contains two seat-related operations:

- `viewSeats(showtimeId: Int!)` asks only for
  `viewer.showtime(id) -> seatingLayout` and the seat fragment.
- `seatingLayout(showtimeId: Int!)` is the checkout flow. It additionally asks
  for movie/theatre/order context plus signed-in user and friend information.

The monitor uses the smaller named `viewSeats` operation and includes the app's
`shouldDisplay` seat field. Hidden cells are excluded from recommendations.
Showtime discovery continues to use the anonymous `movieShowtimes`-equivalent
viewer graph documented in `amc-graphql.md`.

Testing confirmed that AMC accepts eight aliased `showtime` selections in one
request. The worker therefore uses bounded batches and pauses on upstream or
transport failures.

## Reliability decision

Use the official GraphQL origin, one serialized request stream, cached
discovery, bounded alias batches, and hard cooldowns. The monitor does not
change proxy identity in reaction to 403 or 429 responses; those responses
trigger a shared pause.

No login token, checkout token, customer cookie, or payment flow is used or
stored. The monitor only observes public showtime and seating availability and
links to AMC for manual purchase.
