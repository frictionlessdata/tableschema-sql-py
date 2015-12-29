# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import os
import csv
import six
import json
from tabulator import topen, processors
from jsontableschema.model import SchemaModel


# Module API

def import_resource(storage, table, schema_path, data_path, force=False):
    """Import JSONTableSchema resource to storage's table.

    Parameters
    ----------
    storage: object
        Storage object.
    table: str
        Table name.
    schema_path: str
        Path to schema file.
    data_path: str
        Path to data file.

    """

    # Get schema
    with io.open(schema_path, encoding='utf-8') as file:
        schema = json.load(file)

    # Create table
    if storage.check(table):
        if not force:
            message = 'Table %s is already existent' % table
            raise RuntimeError(message)
        storage.delete(table)
    storage.create(table, schema)

    # Write data to table
    with topen(data_path, with_headers=True) as data:
        data.add_processor(processors.Schema(schema))
        storage.write(table, data)


def export_resource(storage, table, schema_path, data_path):
    """Export JSONTableSchema resource from storage's table.
    """

    # Ensure export directories
    for path in [schema_path, data_path]:
        dirpath = os.path.dirname(path)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

    # Write schema on disk
    schema = storage.describe(table)
    with io.open(schema_path,
                 mode=_write_mode,
                 encoding=_write_encoding) as file:
        json.dump(schema, file, indent=4)

    # Write data on disk
    data = storage.read(table)
    model = SchemaModel(schema)
    with io.open(path,
                 mode=_write_mode,
                 newline=_write_newline,
                 encoding=_write_encoding) as file:
        writer = csv.writer(file)
        writer.writerow(model.headers)
        for row in data:
            writer.writerow(row)


# Internal

_write_mode = 'w'
if six.PY2:
    _write_mode = 'wb'

_write_encoding = 'utf-8'
if six.PY2:
    _write_encoding = None

_write_newline = ''
if six.PY2:
    _write_newline = None
