# CHANGELOG

<!-- version list -->

## v1.8.0 (2026-09-09)

### Features

- Add valuematrix_append method to ConI class
  ([#287](https://github.com/peak-solution/odsbox/pull/287),
  [`4cdb091`](https://github.com/peak-solution/odsbox/commit/4cdb0914c09d15abfdc123dd5b688bcbf213c6cf))

- Integrate ASAM ODS v6.2.1 interface definitions
  ([#287](https://github.com/peak-solution/odsbox/pull/287),
  [`4cdb091`](https://github.com/peak-solution/odsbox/commit/4cdb0914c09d15abfdc123dd5b688bcbf213c6cf))

- Regenerate stub with v6.2.1 ([#287](https://github.com/peak-solution/odsbox/pull/287),
  [`4cdb091`](https://github.com/peak-solution/odsbox/commit/4cdb0914c09d15abfdc123dd5b688bcbf213c6cf))

- Update to ASAM ODS v6.2.1 interface ([#287](https://github.com/peak-solution/odsbox/pull/287),
  [`4cdb091`](https://github.com/peak-solution/odsbox/commit/4cdb0914c09d15abfdc123dd5b688bcbf213c6cf))


## v1.7.0 (2026-08-24)

### Bug Fixes

- Vulnerabilities
  ([`7096b02`](https://github.com/peak-solution/odsbox/commit/7096b02c326c7147aea33cf92eecd7be3dcfccfe))

- Vulnerabilities
  ([`08c881d`](https://github.com/peak-solution/odsbox/commit/08c881dd735b87c287d13c27efb47552f868b63c))

### Continuous Integration

- Bump astral-sh/setup-uv from 9.0.0 to 10.0.1
  ([#279](https://github.com/peak-solution/odsbox/pull/279),
  [`6144258`](https://github.com/peak-solution/odsbox/commit/6144258f8887bae248b20bf904b42a086ee8bfa0))

- Bump astral-sh/setup-uv from 9.0.0 to 10.0.1
  ([#279](https://github.com/peak-solution/odsbox/pull/279),
  [`86b1128`](https://github.com/peak-solution/odsbox/commit/86b1128c590044584c7b228fe1fde15fa3b7e0b0))

- Bump gitpython from 3.1.55 to 3.1.58 ([#278](https://github.com/peak-solution/odsbox/pull/278),
  [`cda78fe`](https://github.com/peak-solution/odsbox/commit/cda78feabb162a5096163e8a63759538a3920f38))

- Bump gitpython from 3.1.55 to 3.1.58 ([#278](https://github.com/peak-solution/odsbox/pull/278),
  [`bbc69e5`](https://github.com/peak-solution/odsbox/commit/bbc69e532dbde209a398db581cc9eef33027dced))

### Features

- Add the ability to use localcolumn flags in BulkReader
  ([#280](https://github.com/peak-solution/odsbox/pull/280),
  [`2ddbb51`](https://github.com/peak-solution/odsbox/commit/2ddbb5147b5c87708236051d954a066c52eaa93a))

- Add the ability to use localcolumn flags in BulkReader
  ([#280](https://github.com/peak-solution/odsbox/pull/280),
  [`bad51d8`](https://github.com/peak-solution/odsbox/commit/bad51d8033c9b8ac4400dc7184b73d8c82c8cd02))


## v1.6.0 (2026-07-24)

### Continuous Integration

- Bump actions/setup-python from 6 to 7 ([#273](https://github.com/peak-solution/odsbox/pull/273),
  [`57cc3b9`](https://github.com/peak-solution/odsbox/commit/57cc3b9114da3941edcad42e593c1d784d19b7ff))

- Bump astral-sh/setup-uv from 8.3.2 to 9.0.0
  ([#274](https://github.com/peak-solution/odsbox/pull/274),
  [`1c00872`](https://github.com/peak-solution/odsbox/commit/1c008727eda38dd73c0889c912f4cafdbf70f8d8))

### Features

- Added context utilities
  ([`9447be8`](https://github.com/peak-solution/odsbox/commit/9447be84ba7b0b21024f5e7d2f911fffdf1fe84f))


## v1.5.0 (2026-07-17)

### Bug Fixes

- Move partial_result handling to pandas conversion
  ([#269](https://github.com/peak-solution/odsbox/pull/269),
  [`05d38c7`](https://github.com/peak-solution/odsbox/commit/05d38c7aee42cf54481ea9f0ef0f4a1de74059cb))

- Move partial_result handling to pandas conversion
  ([#266](https://github.com/peak-solution/odsbox/pull/266),
  [`d990868`](https://github.com/peak-solution/odsbox/commit/d990868a837b5ae010d94a4073074fdc7fc3576f))

- Raise PartialResultError when server truncates a response
  ([#269](https://github.com/peak-solution/odsbox/pull/269),
  [`05d38c7`](https://github.com/peak-solution/odsbox/commit/05d38c7aee42cf54481ea9f0ef0f4a1de74059cb))

- Raise PartialResultError when server truncates a response
  ([#266](https://github.com/peak-solution/odsbox/pull/266),
  [`d990868`](https://github.com/peak-solution/odsbox/commit/d990868a837b5ae010d94a4073074fdc7fc3576f))

### Continuous Integration

- Bump actions/checkout from 6 to 7 ([#263](https://github.com/peak-solution/odsbox/pull/263),
  [`dd3965d`](https://github.com/peak-solution/odsbox/commit/dd3965dff61054b02779a63bed57ccd163976204))

- Bump astral-sh/setup-uv from 8.1.0 to 8.2.0
  ([#257](https://github.com/peak-solution/odsbox/pull/257),
  [`e632926`](https://github.com/peak-solution/odsbox/commit/e63292618c8538b9864bed4530636d5903086160))

- Bump astral-sh/setup-uv from 8.2.0 to 8.3.2
  ([#267](https://github.com/peak-solution/odsbox/pull/267),
  [`9d2c2e5`](https://github.com/peak-solution/odsbox/commit/9d2c2e53c35adabb7e383a7c67c3058085f498b7))

### Features

- Add URL validation and construction for OIDC discovery and WebFinger endpoints
  ([`69714c9`](https://github.com/peak-solution/odsbox/commit/69714c924bb07481af9fcae797bec05df084e3cb))

- Implement partial result handling in bulk reader
  ([`f71743d`](https://github.com/peak-solution/odsbox/commit/f71743d781eb069e45273e83d604a685375b688a))


## v1.4.1 (2026-05-07)

### Bug Fixes

- Bump mypy from 1.20.2 to 2.0.0 in the dev-dependencies group
  ([#252](https://github.com/peak-solution/odsbox/pull/252),
  [`33e3b41`](https://github.com/peak-solution/odsbox/commit/33e3b416f35edc0b4970d672b0db813ff9e3211e))

### Documentation

- Google-site-verification is missing in new schema
  ([#252](https://github.com/peak-solution/odsbox/pull/252),
  [`33e3b41`](https://github.com/peak-solution/odsbox/commit/33e3b416f35edc0b4970d672b0db813ff9e3211e))

- Make sure documentation contains correct documentation
  ([#252](https://github.com/peak-solution/odsbox/pull/252),
  [`33e3b41`](https://github.com/peak-solution/odsbox/commit/33e3b416f35edc0b4970d672b0db813ff9e3211e))


## v1.4.0 (2026-05-05)

### Features

- Bump build action versions ([#250](https://github.com/peak-solution/odsbox/pull/250),
  [`4e3b9f6`](https://github.com/peak-solution/odsbox/commit/4e3b9f6ec55bd3fb88f17962a768d10d84fb742d))


## v1.3.0 (2026-05-05)

### Bug Fixes

- Bump mypy from 1.20.0 to 1.20.1 ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))

- Bump mypy from 1.20.1 to 1.20.2 ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))

- Bump pre-commit from 4.5.1 to 4.6.0 ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))

- Bump pytest from 9.0.2 to 9.0.3 ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))

- Bump sphinx-notes/pages from 3.5 to 3.6 ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))

- Precommit checks integrated ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))

### Features

- Switch CI pipeline to uv ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))

- Update build proccess to uv and modernize
  ([#243](https://github.com/peak-solution/odsbox/pull/243),
  [`76387b5`](https://github.com/peak-solution/odsbox/commit/76387b5d52382e43cfc777b441723a2447533b80))
