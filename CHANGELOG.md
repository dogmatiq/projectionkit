# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->

[keep a changelog]: https://keepachangelog.com/en/1.0.0/
[semantic versioning]: https://semver.org/spec/v2.0.0.html
[bc]: https://github.com/dogmatiq/.github/blob/main/VERSIONING.md#changelogs

## [Unreleased]

### Changed

- **[BC]** Added `Driver` parameter to `sqlprojection.New()` and removed `Option` parameters.
- **[BC]** Added `Driver` parameter to `sqlprojection.NewResourceRepository()` and removed `Option` parameters.
- **[BC]** `boltprojection.New()` no longer accepts a `nil` database.
- **[BC]** `dynamoprojection.New()` no longer accepts a `nil` client.
- **[BC]** `sqlprojection.New()` no longer accepts a `nil` database

### Removed

- **[BC]** Removed `sqlprojection.CreateSchema()` and `DropSchema()`, use the corresponding `Driver` method instead.
- **[BC]** Removed `sqlprojection.SelectDriver()`.
- **[BC]** Removed `sqlprojection.BuiltInDrivers()`, `WithDriver()`, `WithCandidateDrivers()` and `sqlprojection.Option`.
- **[BC]** Removed `fixtures` packages.

## [0.7.5] - 2024-09-29

### Changed

- Bumped minimum Go version to 1.23.
- Bumped Dogma to v0.14.3.

## [0.7.4] - 2024-08-17

### Changed

- Bumped Dogma to v0.14.0.

## [0.7.3] - 2024-07-16

### Added

- Added support for `Disable()` method in `dogmatiq/dogma` v0.13.1.

## [0.7.2] - 2024-07-11

### Added

- Added experimental `memoryprojection` package.

## [0.7.1] - 2024-04-01

### Changed

- Updated tests to use Dogma v0.13.0.

## [0.7.0] - 2024-01-17

### Changed

- **[BC]** Changed `resource.RepositoryAware.ResourceRepository()` to accept a `context.Context`

### Fixed

- Fixed issue where `sqlprojection.Options` related to driver selection were ignored
- Fixed issue with SQL driver auto-selection that prevent a database connection from being returned to the pool

## [0.6.5] - 2023-04-09

This release updates the `projectionkit` implementation to adhere to Dogma
v0.12.0 interfaces.

## [0.6.4] - 2023-03-22

### Added

- Added support for DynamoDB projections in the new `dynamoprojection` package
- Added `resource.Repository` interface
- Added `boltprojection.ResourceRepository`
- Added `sqlprojection.ResourceRepository`

### Changed

- The `sqlprojection` MySQL driver no longer uses compressed table rows for the OCC table

## [0.6.3] - 2021-01-29

### Fixed

- Fix MySQL driver detection on old versions of MariaDB

## [0.6.2] - 2020-12-18

### Changed

- `sqlprojection.CreateSchema()` no longer returns an error if the schema already exists

## [0.6.1] - 2020-12-11

### Added

- Add `sqlprojection.CreateSchema()` and `DropSchema()`

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

[unreleased]: https://github.com/dogmatiq/projectionkit
[0.1.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.1.0
[0.2.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.2.0
[0.3.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.3.0
[0.3.1]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.3.1
[0.3.2]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.3.2
[0.4.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.4.0
[0.5.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.5.0
[0.5.1]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.5.1
[0.6.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.6.0
[0.6.1]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.6.1
[0.6.2]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.6.2
[0.6.3]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.6.3
[0.6.4]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.6.4
[0.6.5]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.6.5
[0.7.0]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.7.0
[0.7.1]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.7.1
[0.7.2]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.7.2
[0.7.3]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.7.3
[0.7.4]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.7.4
[0.7.5]: https://github.com/dogmatiq/projectionkit/releases/tag/v0.7.5

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
