# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
from sqlalchemy import (
    Column, PrimaryKeyConstraint, ForeignKeyConstraint,
    Text, VARCHAR,  Float, Integer, Boolean, Date, Time, DateTime)
from sqlalchemy.dialects.postgresql import ARRAY, JSON, JSONB, UUID


# Module API

def bucket_to_tablename(prefix, bucket):
    """Convert bucket to SQLAlchemy tablename.
    """
    return prefix + bucket


def tablename_to_bucket(prefix, tablename):
    """Convert SQLAlchemy tablename to bucket.
    """
    if tablename.startswith(prefix):
        return tablename.replace(prefix, '', 1)
    return None


def descriptor_to_columns_and_constraints(prefix, bucket, descriptor):
    """Convert descriptor to SQLAlchemy columns and constraints.
    """

    # Init
    columns = []
    constraints = []
    tablename = bucket_to_tablename(prefix, bucket)

    # Mapping
    mapping = {
        'string': Text,
        'number': Float,
        'integer': Integer,
        'boolean': Boolean,
        'object': JSONB,
        'array': JSONB,
        'date': Date,
        'time': Time,
        'datetime': DateTime,
        'geojson': JSONB,
    }

    # Fields
    for field in descriptor['fields']:
        try:
            column_type = mapping[field['type']]
        except KeyError:
            message = 'Type "%s" of field "%s" is not supported'
            message = message % (field['type'], field['name'])
            raise TypeError(message)
        nullable = not field.get('constraints', {}).get('required', False)
        column = Column(field['name'], column_type, nullable=nullable)
        columns.append(column)

    # Primary key
    pk = descriptor.get('primaryKey', None)
    if pk is not None:
        if isinstance(pk, six.string_types):
            pk = [pk]
        constraint = PrimaryKeyConstraint(*pk)
        constraints.append(constraint)

    # Foreign keys
    fks = descriptor.get('foreignKeys', [])
    for fk in fks:
        fields = fk['fields']
        if isinstance(fields, six.string_types):
            fields = [fields]
        resource = fk['reference']['resource']
        if resource == '<bucket>':
            tablename = bucket_to_tablename(prefix, fk['reference']['bucket'])
        elif resource != 'self':
            message = 'Supported only "self" and "<bucket>" references.'
            raise ValueError(message)
        references = fk['reference']['fields']
        if isinstance(references, six.string_types):
            references = [references]
        joiner = lambda reference: '.'.join([tablename, reference])
        references = list(map(joiner, references))
        constraint = ForeignKeyConstraint(fields, references)
        constraints.append(constraint)

    return (columns, constraints)


def columns_and_constraints_to_descriptor(prefix, tablename, columns, constraints):
    """Convert SQLAlchemy columns and constraints to descriptor.
    """

    # Init
    schema = {}

    # Mapping
    mapping = {
        Text: 'string',
        VARCHAR: 'string',
        UUID: 'string',
        Float: 'number',
        Integer: 'integer',
        Boolean: 'boolean',
        JSON: 'object',
        JSONB: 'object',
        ARRAY: 'array',
        Date: 'date',
        Time: 'time',
        DateTime: 'datetime',
    }

    # Fields
    fields = []
    for column in columns:
        field_type = None
        for key, value in mapping.items():
            if isinstance(column.type, key):
                field_type = value
        if field_type is None:
            message = 'Type "%s" of column "%s" is not supported'
            message = message % (column.type, column.name)
            raise TypeError(message)
        field = {'name': column.name, 'type': field_type}
        if not column.nullable:
            field['constraints'] = {'required': True}
        fields.append(field)
    schema['fields'] = fields

    # Primary key
    pk = []
    for constraint in constraints:
        if isinstance(constraint, PrimaryKeyConstraint):
            for column in constraint.columns:
                pk.append(column.name)
    if len(pk) > 0:
        if len(pk) == 1:
            pk = pk.pop()
        schema['primaryKey'] = pk

    # Foreign keys
    fks = []
    for constraint in constraints:
        if isinstance(constraint, ForeignKeyConstraint):
            fields = []
            bucket = None
            resource = None
            references = []
            for element in constraint.elements:
                fields.append(element.parent.name)
                references.append(element.column.name)
                if element.column.table.name == tablename:
                    resource = 'self'
                else:
                    bucket = tablename_to_bucket(
                        prefix, element.column.table.name)
            if len(fields) == len(references) == 1:
                fields = fields.pop()
                references = references.pop()
            fk = {
                'fields': fields,
                'reference': {
                    'fields': references,
                }
            }
            if resource is not None:
                fk['reference']['resource'] = resource
            if bucket is not None:
                fk['reference']['resource'] = '<bucket>'
                fk['reference']['bucket'] = bucket
            fks.append(fk)
    if len(fks) > 0:
        schema['foreignKeys'] = fks

    return schema
