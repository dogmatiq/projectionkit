<div align="center">

# Dogma Projection Toolkit

Build [Dogma](https://github.com/dogmatiq/dogma) projections for popular
database systems.

[![Documentation](https://img.shields.io/badge/go.dev-documentation-007d9c?&style=for-the-badge)](https://pkg.go.dev/github.com/dogmatiq/projectionkit)
[![Latest Version](https://img.shields.io/github/tag/dogmatiq/projectionkit.svg?&style=for-the-badge&label=semver)](https://github.com/dogmatiq/projectionkit/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/dogmatiq/projectionkit/ci.yml?style=for-the-badge&branch=main)](https://github.com/dogmatiq/projectionkit/actions/workflows/ci.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/dogmatiq/projectionkit/main.svg?style=for-the-badge)](https://codecov.io/github/dogmatiq/projectionkit)

</div>

The projection toolkit provides a set of adaptors for easily building
[projections](https://github.com/dogmatiq/dogma#projection) using various
database systems and other methods of persistence, without having to implement
the lower-level `dogma.ProjectionMessageHandler` interface.

## Supported targets

- [Amazon DynamoDB](https://aws.amazon.com/dynamodb/)
- [BoltDB](https://github.com/etcd-io/bbolt)
- [MySQL](https://www.mysql.com/) and compatible databases
- [PostgreSQL](https://www.postgresql.org/) and compatible database
- [SQLite](https://www.sqlite.org/index.html)

## Future support

- [openCypher](http://opencypher.org), implemented by [Amazon Neptune](https://aws.amazon.com/neptune/), [Neo4j](https://neo4j.com/), etc (in progress)
- Replicated in-memory projections (planned)

## Testing

This project's tests depend on the Docker stack provided by
[`dogmatiq/sqltest`](https://github.com/dogmatiq/sqltest#readme).
