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
from jtssql import Table


def run(dataset_id='jsontableschema',
        table_id='table_test'):

    # Service
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
    credentials = GoogleCredentials.get_application_default()
    service = build('bigquery', 'v2', credentials=credentials)

    # Table
    project_id = json.load(io.open('.credentials.json', encoding='utf-8'))['project_id']
    table = Table(service, project_id, dataset_id, table_id)

    # Delete
    print('[Delete]')
    print(table.is_existent)
    if table.is_existent:
        table.delete()

    # Create
    print('[Create]')
    print(table.is_existent)
    table.create({'fields': [{'name': 'id', 'type': 'STRING'}]})
    print(table.is_existent)
    print(table.schema)

    # Add data
    print('[Add data]')
    table.add_data([('id1',), ('id2',)])
    print(list(table.get_data()))


if __name__ == '__main__':
    run()
