# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY, JSON, JSONB, UUID


# Module API

class Mapper(object):

    # Public

    def __init__(self, prefix):
        """Mapper to convert/restore FD entities to/from SQL entities.
        """
        self.__prefix = prefix

    def convert_bucket(self, bucket):
        """Convert bucket to SQL.
        """
        return self.__prefix + bucket

    def convert_descriptor(self, bucket, descriptor, index_fields, autoincrement):
        """Convert descriptor to SQL.
        """

        # Prepare
        columns = []
        column_mapping = {}
        constraints = []
        indexes = []
        table_name = self.convert_bucket(bucket)

        # Mapping
        mapping = {
            'string': sa.Text,
            'number': sa.Float,
            'integer': sa.Integer,
            'boolean': sa.Boolean,
            'object': JSONB,
            'array': JSONB,
            'date': sa.Date,
            'time': sa.Time,
            'datetime': sa.DateTime,
            'geojson': JSONB,
        }

        # Autoincrement
        if autoincrement is not None:
            columns.append(sa.Column(
                autoincrement, sa.Integer, autoincrement=True, nullable=False))

        # Fields
        for field in descriptor['fields']:
            try:
                column_type = mapping[field['type']]
            except KeyError:
                message = 'Type "%s" of field "%s" is not supported'
                message = message % (field['type'], field['name'])
                raise TypeError(message)
            nullable = not field.get('constraints', {}).get('required', False)
            column = sa.Column(field['name'], column_type, nullable=nullable)
            columns.append(column)
            column_mapping[field['name']] = column

        # Indexes
        for i, index_definition in enumerate(index_fields):
            name = table_name + '_ix%03d' % i
            index_columns = [column_mapping[field_name] for field_name in index_definition]
            indexes.append(sa.Index(name, *index_columns))

        # Primary key
        pk = descriptor.get('primaryKey', None)
        if pk is not None:
            if isinstance(pk, six.string_types):
                pk = [pk]
        if autoincrement is not None:
            if pk is not None:
                pk = [autoincrement] + pk
            else:
                pk = [autoincrement]
        if pk is not None:
            constraint = sa.PrimaryKeyConstraint(*pk)
            constraints.append(constraint)

        # Foreign keys
        fks = descriptor.get('foreignKeys', [])
        for fk in fks:
            fields = fk['fields']
            resource = fk['reference']['resource']
            foreign_fields = fk['reference']['fields']
            if isinstance(fields, six.string_types):
                fields = [fields]
            if resource != '':
                table_name = self.convert_bucket(resource)
            if isinstance(foreign_fields, six.string_types):
                foreign_fields = [foreign_fields]
            composer = lambda field: '.'.join([table_name, field])
            foreign_fields = list(map(composer, foreign_fields))
            constraint = sa.ForeignKeyConstraint(fields, foreign_fields)
            constraints.append(constraint)

        return (columns, constraints, indexes)

    def restore_bucket(self, table_name):
        """Restore bucket from SQL.
        """
        if table_name.startswith(self.__prefix):
            return table_name.replace(self.__prefix, '', 1)
        return None

    def restore_descriptor(self, table_name, columns, constraints, autoincrement_column):
        """Restore descriptor from SQL.
        """

        # Mapping
        mapping = {
            sa.Text: 'string',
            sa.VARCHAR: 'string',
            UUID: 'string',
            sa.Float: 'number',
            sa.Integer: 'integer',
            sa.Boolean: 'boolean',
            JSON: 'object',
            JSONB: 'object',
            ARRAY: 'array',
            sa.Date: 'date',
            sa.Time: 'time',
            sa.DateTime: 'datetime',
        }

        # Fields
        fields = []
        for column in columns:
            if column.name == autoincrement_column:
                continue
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

        # Primary key
        pk = []
        for constraint in constraints:
            if isinstance(constraint, sa.PrimaryKeyConstraint):
                for column in constraint.columns:
                    if column.name == autoincrement_column:
                        continue
                    pk.append(column.name)

        # Foreign keys
        fks = []
        for constraint in constraints:
            if isinstance(constraint, sa.ForeignKeyConstraint):
                resource = ''
                own_fields = []
                foreign_fields = []
                for element in constraint.elements:
                    own_fields.append(element.parent.name)
                    if element.column.table.name != table_name:
                        resource = self.restore_bucket(element.column.table.name)
                    foreign_fields.append(element.column.name)
                if len(own_fields) == len(foreign_fields) == 1:
                    own_fields = own_fields.pop()
                    foreign_fields = foreign_fields.pop()
                fks.append({
                    'fields': own_fields,
                    'reference': {'resource': resource, 'fields': foreign_fields},
                })

        # Desscriptor
        descriptor = {}
        descriptor['fields'] = fields
        if len(pk) > 0:
            if len(pk) == 1:
                pk = pk.pop()
            descriptor['primaryKey'] = pk
        if len(fks) > 0:
            descriptor['foreignKeys'] = fks

        return descriptor
