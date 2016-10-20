# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import json
import pytest
from copy import deepcopy
from decimal import Decimal
from tabulator import Stream
from jsontableschema import Schema
from sqlalchemy import create_engine
from jsontableschema_sql import Storage
from dotenv import load_dotenv; load_dotenv('.env')


# Tests

def test_storage():

    # Get resources
    articles_descriptor = json.load(io.open('data/articles.json', encoding='utf-8'))
    comments_descriptor = json.load(io.open('data/comments.json', encoding='utf-8'))
    articles_rows = Stream('data/articles.csv', headers=1).open().read()
    comments_rows = Stream('data/comments.csv', headers=1).open().read()

    # Engine
    engine = create_engine(os.environ['DATABASE_URL'])

    # Storage
    storage = Storage(engine=engine, prefix='test_storage_')

    # Delete buckets
    storage.delete()

    # Create buckets
    storage.create(
        ['articles', 'comments'],
        [articles_descriptor, comments_descriptor])

    # Recreate bucket
    storage.create('comments', comments_descriptor, force=True)

    # Write data to buckets
    storage.write('articles', articles_rows)
    storage.write('comments', comments_rows)

    # Create new storage to use reflection only
    storage = Storage(engine=engine, prefix='test_storage_')

    # Create existent bucket
    with pytest.raises(RuntimeError):
        storage.create('articles', articles_descriptor)

    # Assert representation
    assert repr(storage).startswith('Storage')

    # Assert buckets
    assert storage.buckets == ['articles', 'comments']

    # Assert descriptors
    assert storage.describe('articles') == sync_descriptor(articles_descriptor)
    assert storage.describe('comments') == sync_descriptor(comments_descriptor)

    # Assert rows
    assert list(storage.read('articles')) == sync_rows(articles_descriptor, articles_rows)
    assert list(storage.read('comments')) == sync_rows(comments_descriptor, comments_rows)

    # Delete non existent bucket
    with pytest.raises(RuntimeError):
        storage.delete('non_existent')

    # Delete buckets
    storage.delete()


def test_storage_bigdata():

    # Generate schema/data
    descriptor = {'fields': [{'name': 'id', 'type': 'integer'}]}
    rows = [[value,] for value in range(0, 2500)]

    # Push rows
    engine = create_engine(os.environ['DATABASE_URL'])
    storage = Storage(engine=engine, prefix='test_storage_bigdata_')
    storage.create('bucket', descriptor, force=True)
    storage.write('bucket', rows)

    # Pull rows
    assert list(storage.read('bucket')) == rows


def test_storage_bigdata_rollback():

    # Generate schema/data
    descriptor = {'fields': [{'name': 'id', 'type': 'integer'}]}
    rows = [(value,) for value in range(0, 2500)] + [('bad-value',)]

    # Push rows
    engine = create_engine(os.environ['DATABASE_URL'])
    storage = Storage(engine=engine, prefix='test_storage_bigdata_rollback_')
    storage.create('bucket', descriptor, force=True)
    try:
        storage.write('bucket', rows)
    except Exception:
        pass

    # Pull rows
    assert list(storage.read('bucket')) == []


# Helpers

def sync_descriptor(descriptor):
    descriptor = deepcopy(descriptor)
    for field in descriptor['fields']:
        if field['type'] in ['array', 'geojson']:
            field['type'] = 'object'
        if 'format' in field:
            del field['format']
    return descriptor


def sync_rows(descriptor, rows):
    result = []
    schema = Schema(descriptor)
    for row in rows:
        result.append(schema.cast_row(row))
    return result
