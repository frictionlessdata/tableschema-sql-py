# jsontableschema-sql-py

[![Travis](https://img.shields.io/travis/okfn/jsontableschema-sql-py/update.svg)](https://travis-ci.org/okfn/jsontableschema-sql-py)
[![Coveralls](http://img.shields.io/coveralls/okfn/jsontableschema-sql-py/update.svg)](https://coveralls.io/r/okfn/jsontableschema-sql-py?branch=update)

Generate and load SQL tables based on JSON Table Schema descriptors.

## Tabular Storage

Package implements [Tabular Storage](https://github.com/okfn/datapackage-storage-py#tabular-storage) interface.

SQLAlchemy is used as sql wrapper. We can get storage this way:

```python
import jtssql
from sqlalchemy import create_engine

engine = create_engine('sqlite:///:memory:', prefix='prefix')
storage = jtssql.Storage(engine)
```

Then we could interact with storage:

```python
storage.tables
storage.check('table_name') # check existence
storage.create('table_name', shema)
storage.delete('table_name')
storage.describe('table_name') # return schema
storage.read('table_name') # return data
storage.write('table_name', data)
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
- [Storage](https://github.com/okfn/jsontableschema-sql-py/blob/master/jtssql/storage.py)
