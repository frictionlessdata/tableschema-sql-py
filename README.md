# tableschema-sql-py

[![Travis](https://img.shields.io/travis/frictionlessdata/tableschema-sql-py/master.svg)](https://travis-ci.org/frictionlessdata/tableschema-sql-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/tableschema-sql-py/master.svg)](https://coveralls.io/r/frictionlessdata/tableschema-sql-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/tableschema-sql.svg)](https://pypi.python.org/pypi/tableschema-sql)
[![Github](https://img.shields.io/badge/github-master-brightgreen)](https://github.com/frictionlessdata/tableschema-sql-py)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load SQL tables based on [Table Schema](http://specs.frictionlessdata.io/table-schema/) descriptors.

## Features

- implements `tableschema.Storage` interface
- provides additional features like indexes and updating

## Contents

<!--TOC-->

  - [Getting Started](#getting-started)
    - [Installation](#installation)
  - [Documentation](#documentation)
  - [API Reference](#api-reference)
    - [`Storage`](#storage)
  - [Contributing](#contributing)
  - [Changelog](#changelog)

<!--TOC-->

## Getting Started

### Installation

The package use semantic versioning. It means that major versions  could include breaking changes. It's highly recommended to specify `package` version range in your `setup/requirements` file e.g. `package>=1.0,<2.0`.

```bash
pip install tableschema-sql
```

## Documentation

```python
from tableschema import Table
from sqlalchemy import create_engine

# Create sqlalchemy engine
engine = create_engine('sqlite://')

# Save package to SQL
package = Package('datapackage.json')
package.save(storage='sql', engine=engine)

# Load package from SQL
package = Package(storage='sql', engine=engine)
package.resources
```

## API Reference

### `Storage`
```python
Storage(self, engine, dbschema=None, prefix='', reflect_only=None, autoincrement=None)
```
SQL storage

Package implements
[Tabular Storage](https://github.com/frictionlessdata/tableschema-py#storage)
interface (see full documentation on the link):

![Storage](https://i.imgur.com/RQgrxqp.png)

> Only additional API is documented

__Arguments__
- __engine (object)__: `sqlalchemy` engine
- __dbschema (str)__: name of database schema
- __prefix (str)__: prefix for all buckets
- __reflect_only (callable)__:
        a boolean predicate to filter the list of table names when reflecting
- __autoincrement (str/dict)__:
        add autoincrement column at the beginning.
          - if a string it's an autoincrement column name
          - if a dict it's an autoincrements mapping with column
            names indexed by bucket names, for example,
            `{'bucket1': 'id', 'bucket2': 'other_id}`


#### `storage.create`
```python
storage.create(self, bucket, descriptor, force=False, indexes_fields=None)
```
Create bucket

__Arguments__
- __indexes_fields (str[])__:
        list of tuples containing field names, or list of such lists


#### `storage.write`
```python
storage.write(self, bucket, rows, keyed=False, as_generator=False, update_keys=None, buffer_size=1000, use_bloom_filter=True)
```
Write to bucket

__Arguments__
- __keyed (bool)__:
        accept keyed rows
- __as_generator (bool)__:
        returns generator to provide writing control to the client
- __update_keys (str[])__:
        update instead of inserting if key values match existent rows
- __buffer_size (int=1000)__:
        maximum number of rows to try and write to the db in one batch
- __use_bloom_filter (bool=True)__:
        should we use a bloom filter to optimize DB update performance
        (in exchange for some setup time)


## Contributing

> The project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Recommended way to get started is to create and activate a project virtual environment.
To install package and development dependencies into active environment:

```bash
$ make install
```

To run tests with linting and coverage:

```bash
$ make test
```

## Changelog

Here described only breaking and the most important changes. The full changelog and documentation for all released versions could be found in nicely formatted [commit history](https://github.com/frictionlessdata/tableschema-sql-py/commits/master).

#### v1.3

- Implemented constraints loading to a database

#### v1.2

- Add option to configure buffer size, bloom filter use (#77)

#### v1.1

- Added support for the `autoincrement` parameter to be a mapping
- Fixed autoincrement support for SQLite and MySQL

#### v1.0

- Initial driver implementation.
