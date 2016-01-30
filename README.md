# jsontableschema-sql-py

[![Travis](https://img.shields.io/travis/okfn/jsontableschema-sql-py/update.svg)](https://travis-ci.org/okfn/jsontableschema-sql-py)
[![Coveralls](http://img.shields.io/coveralls/okfn/jsontableschema-sql-py/update.svg)](https://coveralls.io/r/okfn/jsontableschema-sql-py?branch=update)

Generate and load SQL tables based on JSON Table Schema descriptors.

## Import/Export

> See section below how to get tabular storage object.

High-level API is easy to use.

Having `schema.json` (JSONTableSchema) and `data.csv` in
current directory we can import it to the sql database:

```python
import jtssql

jtssql.import_resource(<storage>, 'table', 'schema.json', 'data.csv')
```

Also we can export it from sql database:

```python
import jtssql

jtssql.export_resource(<storage>, 'table', 'schema.json', 'data.csv')
```

## Tabular Storage

SQLAlchemy is used as sql wrapper.
We can get storage this way:

```python
import jtssql
from sqlalchemy import create_engine

engine = create_engine('sqlite:///:memory:')
storage = jtssql.Storage(engine)
```

## Mappings

```
schema.json -> SQL table schema
data.csv -> SQL talbe data
```

## Drivers

SQLAlchemy is used - [docs](http://www.sqlalchemy.org/).

## Documentation

API documentation is presented as docstings:
- [import/export](https://github.com/okfn/jsontableschema-sql-py/blob/master/jtssql/resource.py)
- [Storage](https://github.com/okfn/jsontableschema-sql-py/blob/master/jtssql/storage.py)
