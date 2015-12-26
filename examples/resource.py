# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import os
import sys
import json
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

sys.path.insert(0, '.')
from jtssql import Resource


def run(import_schema_path='examples/data/spending/schema.json',
        export_schema_path='tmp/schema.json',
        import_data_path='examples/data/spending/data.csv',
        export_data_path='tmp/data.csv',
        table_id='resource_test'):

    # Service
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)

    # Table
    project_id = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    resource = Resource(service, project_id, 'jsontableschema', table_id)

    # Delete
    print('[Delete]')
    print(resource.is_existent)
    if resource.is_existent:
        resource.delete()

    # Create
    print('[Create]')
    print(resource.is_existent)
    resource.create(import_schema_path)
    print(resource.is_existent)
    print(resource.schema)

    # Add data
    # print('[Add data]')
    # resource.add_data([('id1', 'name1', True, 333.0)])
    # print(list(resource.get_data()))

    # Import data
    print('[Import data]')
    resource.import_data(import_data_path)
    print(list(resource.get_data()))

    # Export schema/data
    print('[Export schema/data]')
    resource.export_schema(export_schema_path)
    resource.export_data(export_data_path)
    print('done')

    return locals()


if __name__ == '__main__':
    run()
