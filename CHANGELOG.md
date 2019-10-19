# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog], and this project adheres to
[Semantic Versioning].

<!-- references -->
[Keep a Changelog]: https://keepachangelog.com/en/1.0.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

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

<!-- version template
## [0.0.1] - YYYY-MM-DD

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security
-->
