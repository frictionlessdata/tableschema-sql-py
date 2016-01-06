# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import os
import sys
import json
from sqlalchemy import create_engine

sys.path.insert(0, '.')
import jtssql


def run(schema={'fields': [{'name': 'id', 'type': 'string'}]},
        data=[('id1',), ('id2',)],
        prefix='test_',
        table='test'):

    # Storage
    engine = create_engine('sqlite:///:memory:')
    storage = jtssql.Storage(engine=engine, prefix=prefix)

    # Delete
    print('[Delete]')
    print(storage.check(table))
    if storage.check(table):
        storage.delete(table)

    # Create
    print('[Create]')
    print(storage.check(table))
    storage.create(table, schema)
    print(storage.check(table))
    print(storage.describe(table))

    # Add data
    print('[Add data]')
    storage.write(table, data)
    print(list(storage.read(table)))

    # Tables
    print('[Tables]')
    print(storage.tables)


if __name__ == '__main__':
    run()
