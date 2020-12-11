# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [0.6.0] - 2020-12-11

As of this release the various SQL projection drivers no longer depend on
specific Go `database/sql` driver implementations. They should each work with
any underlying `database/sql` driver that supports the database's native query
placeholder format (`?` for MySQL, `$1` for everything else).

### Added

- **[BC]** Add `CreateSchema()` and `DropSchema()` methods to `sqlprojection.Driver`
- **[BC]** Add `IsCompatibleWith()` method to `sqlprojection.Driver`
- Add `sqlprojection.Option`
- Add `sqlprojection.SelectDriver()`

### Changed

- **[BC]** Rename `boltdb` package to `boltprojection`
- **[BC]** Rename `sql` package to `sqlprojection`
- **[BC]** `sqlprojection.New()` no longer returns an error
- **[BC]** `sqlprojection.New()` now accepts functional options instead of a single driver
- `boltprojection.New()` and `sqlprojection.New()` now explicitly accept a `nil` database

### Removed

- **[BC]** Remove the `sql/mysql` package
- **[BC]** Remove the `sql/postgres` package
- **[BC]** Remove the `sql/sqlite` package
- **[BC]** Removed `sqlprojection.MustNew()`
- **[BC]** Removed `sqlprojection.NewDriver()`

## [0.5.1] - 2020-11-14

### Added

- Add support for `github.com/jackc/pgx` PostgreSQL driver
- Add `sql.NoCompactBehavior` and `boltdb.NoCompactBehavior` embeddable structs

## [0.5.0] - 2020-11-13

### Changed

- **[BC]** Update to Dogma v0.10.0

### Added

- **[BC]** Add `sql.MessageHandler.Compact()`
- **[BC]** Add `boltdb.MessageHandler.Compact()`

## [0.4.0] - 2020-06-29

### Added

- Add `sql.MustNew()`

## [0.3.2] - 2020-02-28

### Added

- Add test fixtures for BoltDB and SQL based message handlers
- Add the `resource` package for manually manipulating resource versions

## [0.3.1] - 2019-12-24

### Added

- Add the `boltdb` package for building BoltDB-based projections

## [0.3.0] - 2019-10-31

### Added

- Add `mysql.IsCompatibleWith()`
- Add `postgres.IsCompatibleWith()`
- Add `sqlite.IsCompatibleWith()`

### Changed

- Bump EngineKit to v0.8.0

### Fixed

- Fix `driver.New()` to build correctly when CGO is disabled

## [0.2.0] - 2019-10-19

### Added

- Add `sql.NewDriver()` function, which returns the appropriate driver for supported `*sql.DB` instances
- **[BC]** Return an error from `sql.New()`

### Fixed

- **[BC]** Add driver parameter to `sql.New()`

## [0.1.0] - 2019-10-14

- Initial release

<!-- references -->
[Unreleased]: https://github.com/dogmatiq/projectionkit
[0.1.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.1.0
[0.2.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.2.0
[0.3.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.3.0
[0.3.1]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.3.1
[0.3.2]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.3.2
[0.4.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.4.0
[0.5.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.5.0
[0.5.1]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.5.1
[0.6.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.6.0

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
