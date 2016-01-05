# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
from sqlalchemy import Table, Column, MetaData, Text, Integer, Float, Boolean
from jsontableschema.model import SchemaModel


# Module API

class Storage(object):

    # Public

    def __init__(self, engine, dbschema=None, prefix=''):

        # Set attributes
        self.__engine = engine
        self.__dbschema = dbschema
        self.__prefix = prefix

        # Create metadata
        self.__metadata = MetaData(
                bind=self.__engine,
                schema=self.__dbschema,
                reflect=True)

    def __repr__(self):

        # Template and format
        template = 'Storage <{engine}/{dbschema}>'
        text = template.format(
                engine=self.__engine,
                dbschema=self.__dbschema)

        return text

    @property
    def tables(self):

        # Collect
        tables = []
        for dbtable in self.__metadata.tables.values():
            table = dbtable.name
            table = self.__restore_table(table)
            tables.append(table)

        return tables

    def check(self, table):
        return table in self.tables

    def create(self, table, schema):
        """Create table by schema.

        Parameters
        ----------
        table: str/list
            Table name or list of table names.
        schema: dict/list
            JSONTableSchema descriptor or list of them.

        Raises
        ------
        RuntimeError
            If table already exists.

        """

        # Make lists
        tables = table
        if isinstance(table, six.string_types):
            tables = [table]
        schemas = schema
        if isinstance(schema, dict):
            schemas = [schema]

        # Iterate over tables/schemas
        for table, schema in zip(tables, schemas):

            # Check not existent
            if self.check(table):
                message = 'Table "%s" already exists.' % table
                raise RuntimeError(message)

            # Define table
            table = self.__convert_table(table)
            elements = self.__convert_schema(schema)
            Table(table, self.__metadata, *elements)

        # Create tables, update metadata
        self.__metadata.create_all()
        # Metadata reflect is auto

    def delete(self, table):
        """Delete table.

        Parameters
        ----------
        table: str/list
            Table name or list of table names.

        Raises
        ------
        RuntimeError
            If table is not existent.

        """

        # Make lists
        tables = table
        if isinstance(table, six.string_types):
            tables = [table]

        # Iterate over tables
        targets = []
        for table in tables:

            # Check existent
            if not self.check(table):
                message = 'Table "%s" is not existent.' % self
                raise RuntimeError(message)

            # Add table to targets
            table = self.__convert_table(table)
            targets.append(table)

        # Drop tables, update metadata
        self.__metadata.drop_all(tables=[targets])
        self.__metadata.reflect()

    def describe(self, table):

        # Get schema
        dbtable = self.__get_dbtable(table)
        schema = self.__restore_schema(dbtable)

        return schema

    def read(self, table):

        # Get result
        dbtable = self.__get_dbtable(table)
        result = dbtable.select().execute()

        # Get data
        data = []
        schema = self.describe(table)
        model = SchemaModel(schema)
        for row in result:
            row = tuple(model.convert_row(*row))
            data.append(row)

        return data

    def write(self, table, data):

        # Process data
        schema = self.describe(table)
        model = SchemaModel(schema)
        cdata = []
        for row in data:
            rdata = {}
            row = tuple(model.convert_row(*row))
            for index, field in enumerate(model.fields):
                rdata[field['name']] = row[index]
            cdata.append(rdata)

        # Insert data
        dbtable = self.__get_dbtable(table)
        dbtable.insert().execute(cdata)

    # Private

    def __get_dbtable(self, table):
        """Return dbtable instance from metadata.
        """

        # Prepare dict key
        key = self.__convert_table(table)
        if self.__dbschema:
            key = '.'.join(self.__prefix, key)

        return self.__metadata.tables[key]

    def __convert_table(self, table):
        """Convert high-level table name to database name.
        """
        return self.__prefix + table

    def __restore_table(self, table):
        """Restore database table name to high-level name.
        """
        return table.replace(self.__prefix, '', 1)

    def __convert_schema(self, schema):
        """Convert JSONTableSchema schema to SQLAlchemy columns.
        """

        # Mapping
        mapping = {
            'string': Text(),
            'integer': Integer(),
            'number': Float(),
            'boolean': Boolean(),
        }

        # Convert
        elements = []
        for field in schema['fields']:
            try:
                column_type = mapping[field['type']]
            except KeyError:
                message = 'Type %s is not supported' % field['type']
                raise TypeError(message)
            column = Column(field['name'], column_type)
            elements.append(column)

        return elements

    def __restore_schema(self, dbtable):
        """Convert SQLAlchemy table reflection to JSONTableSchema schema.
        """

        # Mapping
        mapping = {
            Text: 'string',
            Integer: 'integer',
            Float: 'number',
            Boolean: 'boolean',
        }

        # Convert
        fields = []
        for column in dbtable.columns:
            try:
                field_type = mapping[column.type.__class__]
            except KeyError:
                message = 'Type %s is not supported' % column.type
                raise TypeError(message)
            field = {'name': column.name, 'type': field_type}
            fields.append(field)
        schema = {'fields': fields}

        return schema
