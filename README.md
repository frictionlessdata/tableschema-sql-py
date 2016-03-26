# jsontableschema-sql-py

[![Travis](https://img.shields.io/travis/frictionlessdata/jsontableschema-sql-py/master.svg)](https://travis-ci.org/frictionlessdata/jsontableschema-sql-py)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/jsontableschema-sql-py/master.svg)](https://coveralls.io/r/frictionlessdata/jsontableschema-sql-py?branch=master)
[![PyPi](https://img.shields.io/pypi/v/jsontableschema-sql.svg)](https://pypi.python.org/pypi/jsontableschema-sql)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

Generate and load SQL tables based on JSON Table Schema descriptors.

## Tabular Storage

Package implements [Tabular Storage](https://github.com/okfn/datapackage-storage-py#tabular-storage) interface.

SQLAlchemy is used as sql wrapper. We can get storage this way:

```python
from sqlalchemy import create_engine
from jsontableschema_sql import Storage

engine = create_engine('sqlite:///:memory:', prefix='prefix')
storage = Storage(engine)
```

Then we could interact with storage:

```python
storage.tables
storage.check('table_name') # check existence
storage.create('table_name', schema)
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
- [Storage](https://github.com/frictionlessdata/jsontableschema-sql-py/blob/master/jsontableschema_sql/storage.py)

## Contributing

Please read the contribution guideline:

[How to Contribute](CONTRIBUTING.md)

Thanks!
