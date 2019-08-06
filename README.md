# tableschema-sql-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-sql-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-sql-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-sql-py/master.svg)](https://coveralls.io/r/frictionlessdata/tableschema-sql-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-sql.svg)](https://pypi.python.org/pypi/tableschema-sql)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load SQL tables based on [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface
- provides additional features like indexes and updating

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Examples](#examples)
  - [Documentation](#documentation)
    - [Storage](#storage)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
pip install tableschema-sql
```

### Examples

Code examples in this readme requires Python 3.3+ interpreter. You could see even more example in [examples](https://github.com/frictionlessdata/tableschema-sql-py/tree/master/examples) directory.

```python
from tableschema import Table
from sqlalchemy import create_engine

# Load and save table to SQL
engine = create_engine('sqlite://')
table = Table('data.csv', schema='schema.json')
table.save('data', storage='sql', engine=engine)
```

## Documentation

The whole public API of this package is described here and follows semantic versioning rules. Everyting outside of this readme are private API and could be changed without any notification on any new version.

### Storage

Package implements [Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage) interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

This driver provides an additional API:

#### `Storage(engine, dbschema=None, prefix='', reflect_only=None, autoincrement=False)`
- `engine (object)` - `sqlalchemy` engine
- `dbschema (str)` - name of database schema
- `prefix (str)` - prefix for all buckets
- `reflect_only (callable)` - a boolean predicate to filter the list of table names when reflecting
- `autoincrement (bool)` - add autoincrement column at the beginning

#### `storage.create(..., indexes_fields=None)`

- `indexes_fields (str[])` - list of tuples containing field names, or list of such lists

#### `storage.write(..., keyed=False, as_generator=False, update_keys=None)`

- `keyed (bool)` - accept keyed rows
- `as_generator (bool)` - returns generator to provide writing control to the client
- `update_keys (str[])` - update instead of inserting if key values match existent rows

## Contributing

The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

For linting `pylama` configured in `pylama.ini` is used. On this stage it's already
installed into your environment and could be used separately with more fine-grained control
as described in documentation - https://pylama.readthedocs.io/en/latest/.

For example to sort results by error type:

```bash
$ pylama --sort <path>
```

For testing `tox` configured in `tox.ini` is used.
It's already installed into your environment and could be used separately with more fine-grained control as described in documentation - https://testrun.org/tox/latest/.

For example to check subset of tests against Python 2 environment with increased verbosity.
All positional arguments and options after `--` will be passed to `py.test`:

```bash
tox -e py27 -- -v tests/<path>
```

Under the hood `tox` uses `pytest` configured in `pytest.ini`, `coverage`
and `mock` packages. This packages are available only in tox envionments.

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-sql-py/commits/master).

#### v1.0

- Added FK support for SQLite databases

#### v0.x

- Initial driver implementation.
