# jts-sql [![Build Status](https://travis-ci.org/okfn/jts-sql.svg?branch=master)](https://travis-ci.org/okfn/jts-sql)

Generate database tables using SQLAlchemy, based on JSON Table Schema field
descriptors. You can also load data into a tables based on an iterator.

## Usage

Here's an example of using ``jts-sql``:

```python
from sqlalchemy import create_engine
from jtssql import SchemaTable

# your rows of data - maybe you loaded these from a CSV :-)
# e.g. data = [ row for row in csv.DictReader(open('mycsv.csv')) ]
data = [
    {'foo': 3, 'bar': 'hello'},
    {'foo': 5, 'bar': 'bye'}
]

# this is your JSON Table Schema - http://dataprotocols.org/json-table-schema/
schema = {
    'fields': [
        {
            'name': 'foo',
            'type': 'integer'
        },
        {
            'name': 'bar',
            'type': 'string'
        }
    ]
}

engine = create_engine('sqlite://example.db')
table = SchemaTable(engine, 'foo_table', schema)
table.create()
table.load_iter(data)

sqla_table = table.table
res = engine.execute(sqla_table.select())
assert data == list(res)
```

## Tests

The test suite will usually be executed in it's own ``virtualenv`` and perform
a coverage check as well as the tests. To execute on a system with
``virtualenv`` and ``make`` installed, type:

```bash
$ make test
```
