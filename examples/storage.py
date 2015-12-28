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
    print(storage.check_table(table))
    if storage.check_table(table):
        storage.delete_table(table)

    # Create
    print('[Create]')
    print(storage.check_table(table))
    storage.create_table(table, {'fields': [{'name': 'id', 'type': 'string'}]})
    print(storage.check_table(table))
    print(storage.describe_table(table))

    # Add data
    print('[Add data]')
    storage.write_table(table, [('id1',), ('id2',)])
    print(list(storage.read_table(table)))

    # Iterator
    print('[List]')
    for table in storage:
        print(table)


if __name__ == '__main__':
    run()
