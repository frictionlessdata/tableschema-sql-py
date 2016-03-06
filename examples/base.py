# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
from sqlalchemy import create_engine

sys.path.insert(0, '.')
from jsontableschema_sql import Storage


def run(url, prefix, table, schema, data):

    # Storage
    engine = create_engine(url)
    storage = Storage(engine=engine, prefix=prefix)

    # Check table
    if storage.check(table):
        # Delete table
        storage.delete(table)

    # Create table
    storage.create(table, schema)

    # Write data to table
    storage.write(table, data)

    # List tables
    tables = storage.tables

    # Describe table
    schema = storage.describe(table)

    # Read data from table
    data = list(storage.read(table))

    return tables, schema, data
