# jsontableschema-bigquery-py

[![Travis](https://img.shields.io/travis/okfn/jsontableschema-bigquery-py.svg)](https://travis-ci.org/okfn/jsontableschema-bigquery-py)
[![Coveralls](http://img.shields.io/coveralls/okfn/jsontableschema-bigquery-py.svg?branch=master)](https://coveralls.io/r/okfn/jsontableschema-bigquery-py?branch=master)

Generate and load SQL tables based on JSON Table Schema descriptors.

## Usage

This section is intended to be used by end-users of the library.

### Import/Export

> See section below how to get SQL engine.

High-level API is easy to use.

Having `schema.json` (JSONTableSchema) and `data.csv` in
current directory we can import it to sql database:

```python
import jtssql

storage = jtssql.Storage(<engine>)
jtssql.import_resource(storage, 'table', 'schema.json', 'data.csv')
```

As well as we can export sql database:

```python
import jtssql

storage = jtssql.Storage(<engine>)
jtssql.export_resource(storage, 'table', 'schema.json', 'data.csv')
```

### SQL Engine

SQLAlchemy is used as sql wrapper. We can get engine this way:

```python
from sqlalchemy import create_engine

engine = create_engine('sqlite:///:memory:')
```

### Design Overview


#### Storage

On level between the high-level interface and SQL wrapper
package uses Tabular Storage concept:

```
Storage
    + init (**options)
    + check (table)
    + create (table, schema)
    + delete (table)
    + describe (table)
    + read (table)
    + write (table, data)
```

#### Mappings

```
schema.json -> SQL table schema
data.csv -> SQL talbe data
```

#### Drivers

SQLAlchemy is used - [docs](http://www.sqlalchemy.org/).

### Documentation

API documentation is presented as docstings:
- [import/export](https://github.com/okfn/jsontableschema-sql-py/blob/master/jtssql/resource.py)
- [Storage](https://github.com/okfn/jsontableschema-sql-py/blob/master/jtssql/table.py)

## Development

This section is intended to be used by tech users collaborating
on this project.

### Getting Started

To activate virtual environment, install
dependencies, add pre-commit hook to review and test code
and get `run` command as unified developer interface:

```
$ source activate.sh
```

### Reviewing

The project follow the next style guides:
- [Open Knowledge Coding Standards and Style Guide](https://github.com/okfn/coding-standards)
- [PEP 8 - Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/)

To check the project against Python style guide:

```
$ run review
```

### Testing

To run tests with coverage check:

```
$ run test
```

Coverage data will be in the `.coverage` file.
