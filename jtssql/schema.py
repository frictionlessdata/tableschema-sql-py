# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals


# Module API

def table2resource(schema):
    return _convert_schema(schema, _TABLE2RESOURCE_MAPPING)


def resource2table(schema):
    return _convert_schema(schema, _RESOURCE2TABLE_MAPPING)


# Internal

_TABLE2RESOURCE_MAPPING = {
    'STRING': 'string',
    'INTEGER': 'integer',
    'FLOAT': 'number',
    'BOOLEAN': 'boolean',
    'TIMESTAMP': 'datetime',
}
_RESOURCE2TABLE_MAPPING = {
    'string': 'STRING',
    'integer': 'INTEGER',
    'number': 'FLOAT',
    'boolean': 'BOOLEAN',
    'datetime': 'TIMESTAMP',
}


def _convert_schema(schema, mapping):
    fields = []
    for field in schema['fields']:
        try:
            ftype = mapping[field['type']]
        except KeyError:
            message = 'Type %s is not supported' % field['type']
            raise TypeError(message)
        fields.append({
            'name': field['name'],
            'type': ftype,
        })
    schema = {'fields': fields}
    return schema
