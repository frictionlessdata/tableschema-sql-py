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


def run(import_schema_path='examples/data/spending/schema.json',
        export_schema_path='tmp/schema.json',
        import_data_path='examples/data/spending/data.csv',
        export_data_path='tmp/data.csv',
        prefix='test_', table='test'):

    # Storage
    engine = create_engine('sqlite:///:memory:')
    storage = jtssql.Storage(engine=engine, prefix=prefix)

    # Import
    print('[Import]')
    jtssql.import_resource(
            storage=storage, table=table,
            schema_path=import_schema_path,
            data_path=import_data_path)
    print('imported')

    # Export
    print('[Export]')
    jtssql.export_resource(
            storage=storage, table=table,
            schema_path=export_schema_path,
            data_path=export_data_path)
    print('exported')


if __name__ == '__main__':
    run()
