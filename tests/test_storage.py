# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import json
import pytest
import tableschema
from copy import deepcopy
from tabulator import Stream
from sqlalchemy import create_engine
from tableschema_sql import Storage
from dotenv import load_dotenv; load_dotenv('.env')


# Resources

ARTICLES = {
    'schema': {
        'fields': [
            {'name': 'id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'parent', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
            {'name': 'current', 'type': 'boolean'},
            {'name': 'rating', 'type': 'number'},
        ],
        'primaryKey': 'id',
        'foreignKeys': [
            {'fields': 'parent', 'reference': {'resource': '', 'fields': 'id'}},
        ],
    },
    'data': [
        ['1', '', 'Taxes', 'True', '9.5'],
        ['2', '1', '中国人', 'False', '7'],
    ],
}
COMMENTS = {
    'schema': {
        'fields': [
            {'name': 'entry_id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'comment', 'type': 'string'},
            {'name': 'note', 'type': 'any'},
        ],
        'primaryKey': 'entry_id',
        'foreignKeys': [
            {'fields': 'entry_id', 'reference': {'resource': 'articles', 'fields': 'id'}},
        ],
    },
    'data': [
        ['1', 'good', 'note1'],
        ['2', 'bad', 'note2'],
    ],
}
TEMPORAL = {
    'schema': {
        'fields': [
            {'name': 'date', 'type': 'date'},
            {'name': 'date_year', 'type': 'date', 'format': '%Y'},
            {'name': 'datetime', 'type': 'datetime'},
            {'name': 'duration', 'type': 'duration'},
            {'name': 'time', 'type': 'time'},
            {'name': 'year', 'type': 'year'},
            {'name': 'yearmonth', 'type': 'yearmonth'},
        ],
    },
    'data': [
        ['2015-01-01', '2015', '2015-01-01T03:00:00Z', 'P1Y1M', '03:00:00', '2015', '2015-01'],
        ['2015-12-31', '2015', '2015-12-31T15:45:33Z', 'P2Y2M', '15:45:33', '2015', '2015-01'],
    ],
}
LOCATION = {
    'schema': {
        'fields': [
            {'name': 'location', 'type': 'geojson'},
            {'name': 'geopoint', 'type': 'geopoint'},
        ],
    },
    'data': [
        ['{"type": "Point","coordinates":[33.33,33.33]}', '30,75'],
        ['{"type": "Point","coordinates":[50.00,50.00]}', '90,45'],
    ],
}
COMPOUND = {
    'schema': {
        'fields': [
            {'name': 'stats', 'type': 'object'},
            {'name': 'persons', 'type': 'array'},
        ],
    },
    'data': [
        ['{"chars":560}', '["Mike", "John"]'],
        ['{"chars":970}', '["Paul", "Alex"]'],
    ],
}


# Tests

def test_storage():

    # Create storage
    engine = create_engine(os.environ['POSTGRES_URL'])
    storage = Storage(engine=engine, prefix='test_storage_')

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create(
        ['articles', 'comments'],
        [ARTICLES['schema'], COMMENTS['schema']],
        indexes_fields=[[['rating'], ['name']], []])
    storage.create('comments', COMMENTS['schema'], force=True)
    storage.create('temporal', TEMPORAL['schema'])
    storage.create('location', LOCATION['schema'])
    storage.create('compound', COMPOUND['schema'])

    # Write data
    storage.write('articles', ARTICLES['data'])
    storage.write('comments', COMMENTS['data'])
    storage.write('temporal', TEMPORAL['data'])
    storage.write('location', LOCATION['data'])
    storage.write('compound', COMPOUND['data'])

    # Create new storage to use reflection only
    storage = Storage(engine=engine, prefix='test_storage_')

    # Create existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.create('articles', ARTICLES['schema'])

    # Assert buckets
    assert storage.buckets == ['articles', 'compound', 'location', 'temporal', 'comments']

    # Assert schemas
    assert storage.describe('articles') == ARTICLES['schema']
    assert storage.describe('comments') == {
        'fields': [
            {'name': 'entry_id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'comment', 'type': 'string'},
            {'name': 'note', 'type': 'string'}, # type downgrade
        ],
        'primaryKey': 'entry_id',
        'foreignKeys': [
            {'fields': 'entry_id', 'reference': {'resource': 'articles', 'fields': 'id'}},
        ],
    }
    assert storage.describe('temporal') == {
        'fields': [
            {'name': 'date', 'type': 'date'},
            {'name': 'date_year', 'type': 'date'}, # format removal
            {'name': 'datetime', 'type': 'datetime'},
            {'name': 'duration', 'type': 'string'}, # type fallback
            {'name': 'time', 'type': 'time'},
            {'name': 'year', 'type': 'integer'}, # type downgrade
            {'name': 'yearmonth', 'type': 'string'}, # type fallback
        ],
    }
    assert storage.describe('location') == {
        'fields': [
            {'name': 'location', 'type': 'object'}, # type downgrade
            {'name': 'geopoint', 'type': 'string'}, # type fallback
        ],
    }
    assert storage.describe('compound') == {
        'fields': [
            {'name': 'stats', 'type': 'object'},
            {'name': 'persons', 'type': 'string'}, # type fallback
        ],
    }

    # Assert data
    assert storage.read('articles') == cast(ARTICLES)['data']
    assert storage.read('comments') == cast(COMMENTS)['data']
    assert storage.read('temporal') == cast(TEMPORAL, skip=['duration', 'yearmonth'])['data']
    assert storage.read('location') == cast(LOCATION, skip=['geopoint'])['data']
    assert storage.read('compound') == cast(COMPOUND, skip=['array'])['data']

    # Assert data with forced schema
    storage.describe('compound', COMPOUND['schema'])
    assert storage.read('compound') == cast(COMPOUND)['data']

    # Delete non existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.delete('non_existent')

    # Delete buckets
    storage.delete()


@pytest.mark.parametrize('dialect, database_url', [
    ('sqlite', os.environ['SQLITE_URL']),
    ('mysql', os.environ['MYSQL_URL']),
])
def test_storage_limited_databases(dialect, database_url):

    # Create storage
    engine = create_engine(database_url)
    storage = Storage(engine=engine, prefix='test_storage_')

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create(
        ['articles', 'comments'],
        [ARTICLES['schema'], COMMENTS['schema']],
        indexes_fields=[[['rating'], ['name']], []])
    storage.create('comments', COMMENTS['schema'], force=True)
    storage.create('temporal', TEMPORAL['schema'])
    storage.create('location', LOCATION['schema'])
    storage.create('compound', COMPOUND['schema'])

    # Write data
    storage.write('articles', ARTICLES['data'])
    storage.write('comments', COMMENTS['data'])
    storage.write('temporal', TEMPORAL['data'])
    storage.write('location', LOCATION['data'])
    storage.write('compound', COMPOUND['data'])

    # Create new storage to use reflection only
    storage = Storage(engine=engine, prefix='test_storage_')

    # Create existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.create('articles', ARTICLES['schema'])

    # Assert buckets
    assert storage.buckets == ['articles', 'comments', 'compound', 'location', 'temporal']

    # Assert schemas
    assert storage.describe('articles') == {
        'fields': [
            {'name': 'id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'parent', 'type': 'integer'},
            {'name': 'name', 'type': 'string'},
            {'name': 'current', 'type': 'boolean' if dialect == 'sqlite' else 'integer'},
            {'name': 'rating', 'type': 'number'},
        ],
        'primaryKey': 'id',
        # foreignKeys not supported
    }
    assert storage.describe('comments') == {
        'fields': [
            {'name': 'entry_id', 'type': 'integer', 'constraints': {'required': True}},
            {'name': 'comment', 'type': 'string'},
            {'name': 'note', 'type': 'string'}, # type downgrade
        ],
        'primaryKey': 'entry_id',
        # foreignKeys not supported
    }
    assert storage.describe('temporal') == {
        'fields': [
            {'name': 'date', 'type': 'date'},
            {'name': 'date_year', 'type': 'date'}, # format removal
            {'name': 'datetime', 'type': 'datetime'},
            {'name': 'duration', 'type': 'string'}, # type fallback
            {'name': 'time', 'type': 'time'},
            {'name': 'year', 'type': 'integer'}, # type downgrade
            {'name': 'yearmonth', 'type': 'string'}, # type fallback
        ],
    }
    assert storage.describe('location') == {
        'fields': [
            {'name': 'location', 'type': 'string'}, # type fallback
            {'name': 'geopoint', 'type': 'string'}, # type fallback
        ],
    }
    assert storage.describe('compound') == {
        'fields': [
            {'name': 'stats', 'type': 'string'}, # type fallback
            {'name': 'persons', 'type': 'string'}, # type fallback
        ],
    }

    # Assert data
    assert storage.read('articles') == cast(ARTICLES)['data']
    assert storage.read('comments') == cast(COMMENTS)['data']
    assert storage.read('temporal') == cast(TEMPORAL, skip=['duration', 'yearmonth'])['data']
    assert storage.read('location') == cast(LOCATION, skip=['geojson', 'geopoint'])['data']
    assert storage.read('compound') == cast(COMPOUND, skip=['array', 'object'])['data']

    # Assert data with forced schema
    storage.describe('compound', COMPOUND['schema'])
    assert storage.read('compound') == cast(COMPOUND)['data']

    # Delete non existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.delete('non_existent')

    # Delete buckets
    storage.delete()


def test_storage_write_generator():

    # Create storage
    engine = create_engine(os.environ['SQLITE_URL'])
    storage = Storage(engine=engine, prefix='test_storage_')

    # Create bucket
    storage.create('comments', COMMENTS['schema'], force=True)

    # Write data using generator
    gen = storage.write('comments', COMMENTS['data'], as_generator=True)
    res = list(gen)

    # Assert
    assert len(res) == 2
    assert storage.read('comments') == cast(COMMENTS)['data']


def test_storage_update():
    RESOURCE = {
        'schema': {
            'fields': [
                {'name': 'person_id', 'type': 'integer', 'constraints': {'required': True}},
                {'name': 'name', 'type': 'string', 'constraints': {'required': True}},
                {'name': 'favorite_color', 'type': 'string'},

            ],
            'primaryKey': 'person_id',
        },
        'data': [
            ['1', 'ulysses', 'blue'],
            ['2', 'theseus', 'green'],
            ['3', 'perseus', 'red'],
            ['4', 'dedalus', 'yellow'],
        ],
        'updateData': [
            ['5', 'apollo', 'orange'],
            ['3', 'perseus', 'magenta'],
            ['6', 'zeus', 'grey'],
            ['4', 'dedalus', 'sunshine',],
            ['5', 'apollo', 'peach'],
        ],
    }

    # Create storage
    update_keys = ['person_id', 'name']
    engine = create_engine(os.environ['POSTGRES_URL'])
    storage = Storage(engine=engine, prefix='test_update_', autoincrement='__id')

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create('colors', RESOURCE['schema'])

    # Write data to buckets
    storage.write('colors', RESOURCE['data'], update_keys=update_keys)
    gen = storage.write('colors', RESOURCE['updateData'],
        update_keys=update_keys, as_generator=True)
    gen = list(gen)
    assert len(gen) == 5
    assert len(list(filter(lambda i: i.updated, gen))) == 3
    assert list(map(lambda i: i.updated_id, gen)) == [5, 3, 6, 4, 5]

    # Reflect storage
    storage = Storage(engine=engine, prefix='test_update_', autoincrement='__id')
    gen = storage.write('colors', RESOURCE['updateData'],
        update_keys=update_keys, as_generator=True)
    gen = list(gen)
    assert len(gen) == 5
    assert len(list(filter(lambda i: i.updated, gen))) == 5
    assert list(map(lambda i: i.updated_id, gen)) == [5, 3, 6, 4, 5]

    # Create new storage to use reflection only
    storage = Storage(engine=engine, prefix='test_update_')

    # Assert data
    rows = list(storage.iter('colors'))
    assert len(rows) == 6
    color_by_person = dict((row[1], row[3]) for row in rows)
    assert color_by_person == {
        1: 'blue',
        2: 'green',
        3: 'magenta',
        4: 'sunshine',
        5: 'peach',
        6: 'grey'
    }

    # Storage without autoincrement
    storage = Storage(engine=engine, prefix='test_update_')
    storage.delete()
    storage.create('colors', RESOURCE['schema'])
    storage.write('colors', RESOURCE['data'], update_keys=update_keys)
    gen = storage.write('colors', RESOURCE['updateData'],
        update_keys=update_keys, as_generator=True)
    gen = list(gen)
    assert len(gen) == 5
    assert len(list(filter(lambda i: i.updated, gen))) == 3
    assert list(map(lambda i: i.updated_id, gen)) == [None, None, None, None, None]


def test_storage_bad_type():
    RESOURCE = {
        'schema': {
            'fields': [
                {'name': 'bad_field', 'type': 'bad_type'}
            ],
        },
        'data': []
    }

    # Create bucket
    engine = create_engine(os.environ['POSTGRES_URL'])
    storage = Storage(engine=engine, prefix='test_bad_type_')
    with pytest.raises(tableschema.exceptions.ValidationError):
        storage.create('bad_type', RESOURCE['schema'], force=True)


def test_storage_only_parameter():
    RESOURCE = {
        'schema': {
            'fields': [
                {'name': 'person_id', 'type': 'integer', 'constraints': {'required': True}},
                {'name': 'name', 'type': 'string'},
            ],
            'primaryKey': 'person_id',
        },
        'data': []
    }

    # Create storage
    engine = create_engine(os.environ['POSTGRES_URL'], echo=True)
    storage = Storage(engine=engine, prefix='test_only_')

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create('names', RESOURCE['schema'], indexes_fields=[['person_id']])

    # Recreate storage limiting reflection
    only = lambda table: 'name' not in table
    engine = create_engine(os.environ['POSTGRES_URL'], echo=True)
    storage = Storage(engine=engine, prefix='test_only_', reflect_only=only)

    # Delete non existent bucket
    with pytest.raises(tableschema.exceptions.StorageError):
        storage.delete('names')


def test_storage_bigdata():
    RESOURCE = {
        'schema': {
            'fields': [
                {'name': 'id', 'type': 'integer'}
            ]
        },
        'data': [{'id': value} for value in range(0, 2500)]
    }

    # Write data
    engine = create_engine(os.environ['POSTGRES_URL'])
    storage = Storage(engine=engine, prefix='test_storage_bigdata_')
    storage.create('bucket', RESOURCE['schema'], force=True)
    storage.write('bucket', RESOURCE['data'], keyed=True)

    # Read data
    assert list(storage.read('bucket')) == list(map(lambda x: [x['id']], RESOURCE['data']))


def test_storage_bigdata_rollback():
    RESOURCE = {
        'schema': {
            'fields': [
                {'name': 'id', 'type': 'integer'}
            ]
        },
        'data': [(value,) for value in range(0, 2500)] + [('bad-value',)]
    }

    # Write data
    engine = create_engine(os.environ['POSTGRES_URL'])
    storage = Storage(engine=engine, prefix='test_storage_bigdata_rollback_')
    storage.create('bucket', RESOURCE['schema'], force=True)
    try:
        storage.write('bucket', RESOURCE['data'])
    except Exception:
        pass

    # Read data
    assert list(storage.read('bucket')) == []


# Helpers

def cast(resource, skip=[]):
    resource = deepcopy(resource)
    schema = tableschema.Schema(resource['schema'])
    for row in resource['data']:
        for index, field in enumerate(schema.fields):
            if field.type not in skip:
                row[index] = field.cast_value(row[index])
    return resource
