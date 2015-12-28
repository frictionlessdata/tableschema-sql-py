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


def run(name='test', prefix='test_'):

    # Storage
    engine = create_engine('sqlite:///:memory:')
    storage = Storage(engine=engine, prefix=prefix)

    # Delete
    print('[Delete]')
    print(storage.check_node(name))
    if storage.check_node(name):
        storage.delete_node(name)

    # Create
    print('[Create]')
    print(storage.check_node(name))
    storage.create_node(name, {'fields': [{'name': 'id', 'type': 'string'}]})
    print(storage.check_node(name))
    print(storage.describe_node(name))

    # Add data
    print('[Add data]')
    storage.write_node(name, [('id1',), ('id2',)])
    print(list(storage.read_node(name)))

    # Iterator
    print('[List]')
    for node in storage:
        print(node)


if __name__ == '__main__':
    run()
