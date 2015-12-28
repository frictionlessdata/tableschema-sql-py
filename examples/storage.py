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
from jtssql import Storage


def run(table='test', prefix='test_'):

    # Storage
    engine = create_engine('sqlite:///:memory:')
    storage = Storage(engine=engine, prefix=prefix)

    # Delete
    print('[Delete]')
    print(storage.check(table))
    if storage.check(table):
        storage.delete(table)

    # Create
    print('[Create]')
    print(storage.check(table))
    storage.create(table, {'fields': [{'name': 'id', 'type': 'string'}]})
    print(storage.check(table))
    print(storage.describe(table))

    # Add data
    print('[Add data]')
    storage.write(table, [('id1',), ('id2',)])
    print(list(storage.read(table)))

    # Iterator
    print('[List]')
    for table in storage:
        print(table)


if __name__ == '__main__':
    run()
