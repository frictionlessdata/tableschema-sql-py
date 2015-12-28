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
from jtssql import Databox


def run(room='text', name='test'):

    # Databox
    engine = create_engine('sqlite:///:memory:')
    databox = Databox(room, name, engine=engine)

    # List
    print('[List]')
    print(Databox.list(room, engine=engine))

    # Delete
    print('[Delete]')
    print(databox.existent)
    if databox.existent:
        databox.delete()

    # Create
    print('[Create]')
    print(databox.existent)
    databox.create({'fields': [{'name': 'id', 'type': 'string'}]})
    print(databox.existent)
    print(databox.schema)

    # Add data
    print('[Add data]')
    databox.add_data([('id1',), ('id2',)])
    print(list(databox.get_data()))


if __name__ == '__main__':
    run()
