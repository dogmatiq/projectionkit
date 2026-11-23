<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://dogmatiq.io/res/img/dynamic/repo/projectionkit/banner/dark.svg">
  <img alt="Dogmatiq projectionkit" src="https://dogmatiq.io/res/img/dynamic/repo/projectionkit/banner/light.svg" height="44">
</picture>

Build [Dogma](https://github.com/dogmatiq/dogma) projections for popular
database systems.

[![API reference](https://dogmatiq.io/res/img/dynamic/repo/projectionkit/badge/docs.svg)](https://pkg.go.dev/github.com/dogmatiq/projectionkit)
[![Latest version](https://dogmatiq.io/res/img/dynamic/repo/projectionkit/badge/semver.svg)](https://github.com/dogmatiq/projectionkit/releases)
[![Build status](https://dogmatiq.io/res/img/dynamic/repo/projectionkit/badge/build.svg)](https://github.com/dogmatiq/projectionkit/actions/workflows/ci.yml)
[![Code coverage](https://dogmatiq.io/res/img/dynamic/repo/projectionkit/badge/coverage.svg)](https://codecov.io/github/dogmatiq/projectionkit)

</div>

The projection toolkit provides a set of adaptors for easily building
[projections](https://github.com/dogmatiq/dogma#projection) using various
database systems and other methods of persistence, without having to implement
the lower-level `dogma.ProjectionMessageHandler` interface.

## Supported targets

- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- [BoltDB](https://github.com/etcd-io/bbolt)
- [MySQL](https://www.mysql.com/) and compatible databases
- [PostgreSQL](https://www.postgresql.org/) and compatible databases
- [SQLite](https://www.sqlite.org/index.html)

## Future support

- [openCypher](http://opencypher.org), implemented by [Amazon Neptune](https://aws.amazon.com/neptune/), [Neo4j](https://neo4j.com/), etc (in progress)
- Replicated in-memory projections (planned)
