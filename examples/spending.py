# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
from pprint import pprint

sys.path.insert(0, '.')
from examples.base import run


# Fixtures
url = 'sqlite:///:memory:'
prefix = 'spending_'
table = 'data'
schema = {
    'fields': [
        {
            'name': 'id',
            'type': 'string',
            'constraints': {
                'required': True
            },
        },
        {
            'name': 'parent',
            'type': 'string',
        },
        {
            'name': 'name',
            'type': 'string',
        },
        {
            'name': 'current',
            'type': 'boolean',
        },
        {
            'name': 'amount',
            'type': 'number',
        },
    ],
    'primaryKey': 'id',
    'foreignKeys': [
        {
            'fields': 'parent',
            'reference': {
                'resource': 'self',
                'fields': 'id',
            },
        },
    ]
}
data = [
    ('A3001', 'A3001', 'Taxes', True, 10000.5),
    ('A5032', 'A3001', 'Parking Fees', False, 2000.5),
]


# Execution
if __name__ == '__main__':
    pprint(run(url, prefix, table, schema, data))
