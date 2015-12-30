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

# TODO: add support to data is list?
def import_resource(storage, table, schema, data, force=False):
    """Import JSONTableSchema resource to storage's table.

    Parameters
    ----------
    storage: object
        Storage object.
    table: str
        Table name.
    schema: dict/str
        jSONTableSchema dict or path to schema file.
    data: str
        Path to data file.
    force: bool
        Force table rewriting If it already exists.

    """

    # Get schema
    if isinstance(schema, six.string_types):
        with io.open(schema, encoding='utf-8') as file:
            schema = json.load(file)

    # Create table
    if storage.check(table):
        if not force:
            message = 'Table %s is already existent' % table
            raise RuntimeError(message)
        storage.delete(table)
    storage.create(table, schema)

    # Write data to table
    with topen(data, with_headers=True) as data:
        data.add_processor(processors.Schema(schema))
        storage.write(table, data)


def export_resource(storage, table, schema, data):
    """Export JSONTableSchema resource from storage's table.

    Parameters
    ----------
    storage: object
        Storage object.
    table: str
        Table name.
    schema: dict/str
        Dict to store schema or path to schema file.
    data: str
        Path to data file.

    """

    # Update schema dict
    if not isinstance(schema, six.string_types):
        schema.update(storage.describe(table))
    # Write schema on disk
    else:
        _ensure_dir(schema)
        with io.open(schema,
                     mode=_write_mode,
                     encoding=_write_encoding) as file:
            schema = storage.describe(table)
            json.dump(schema, file, indent=4)

    # Write data on disk
    _ensure_dir(data)
    with io.open(data,
                 mode=_write_mode,
                 newline=_write_newline,
                 encoding=_write_encoding) as file:
        model = SchemaModel(schema)
        data = storage.read(table)
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


def _ensure_dir(path):
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)
