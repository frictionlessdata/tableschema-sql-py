# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import csv
import time
from sqlalchemy import Table, Column, MetaData, Text, TEXT, Integer, Float, Boolean
from sqlalchemy.sql import select
from sqlalchemy.schema import CreateSchema
from sqlalchemy.exc import OperationalError
from jsontableschema.model import SchemaModel


# Module API

class Databox(object):
    """SQL table gateway.

    Parameters
    ----------
    engine: object
        SQLAlchemy engine.
    dbschema: str
        SQL schema identifier.
    table_id: str
        SQL table identifier.

    """

    # Public

    def __init__(self, room, name, engine, dbschema=None):

        # Set attributes
        self.__room = room
        self.__name = name
        self.__engine = engine
        self.__dbschema = dbschema
        self.__schema = None

    def __repr__(self):

        # Template and format
        template = 'Databox <{name} on {engine}/{dbschema}>'
        text = template.format(
                name = self.__name,
                engine=self.__engine,
                dbschema=dbschema)

        return text

    @classmethod
    def list(cls, room, engine, dbschema=None):
        names = []
        for name in engine.table_names(schema=dbschema):
            if name.startswith('room'+'___'):
                names.append(name)
        return names

    def create(self, schema):
        """Create table by schema.

        Parameters
        ----------
        schema: dict
            BigQuery schema descriptor.

        Raises
        ------
        RuntimeError
            If table is already existent.

        """

        # Check not existent
        if self.existent:
            message = 'Table "%s" is already existent.' % self
            raise RuntimeError(message)

        # Convert jts schema
        columns = self.__convert_schema(schema)

        # Create schema
        if self.__dbschema is not None:
            self.__engine.execute(CreateSchema(self.__dbschema))

        # Create table
        metadata = MetaData()
        table = Table(self.__name, metadata, *columns, schema=self.__dbschema)
        table.create(self.__engine)

    def delete(self):
        """Delete table.

        Raises
        ------
        RuntimeError
            If table is not existent.

        """

        # Check existent
        if not self.existent:
            message = 'Table "%s" is not existent.' % self
            raise RuntimeError(message)

        # Drop table
        metadata = MetaData()
        table = Table(self.__name, metadata,
                autoload=True, autoload_with=self.__engine,
                schema=self.__dbschema)
        table.drop(self.__engine)

        # Remove schema cache
        self.__schema = None

    @property
    def existent(self):
        """Return if databox is existent.
        """

        try:
            return self.__engine.dialect.has_table(
                    self.__engine.connect(), self.__name,
                    schema=self.__dbschema)
        except OperationalError:
            return False

    @property
    def schema(self):
        """Return schema dict.
        """

        # Create cache
        if getattr(self, '__schema', None) is None:

            metadata = MetaData()
            table = Table(self.__name, metadata,
                    autoload=True, autoload_with=self.__engine,
                    schema=self.__dbschema)

            # Get schema
            self.__schema = self.__restore_schema(table)

        return self.__schema

    def add_data(self, data):
        """Add data to table.

        Parameters
        ----------
        data: list
            List of data tuples.

        """

        metadata = MetaData()
        table = Table(self.__name, metadata,
                autoload=True, autoload_with=self.__engine,
                schema=self.__dbschema)

        # Get model and data
        model = SchemaModel(self.schema)
        cdata = []
        for row in data:
            rdata = {}
            row = tuple(model.convert_row(*row))
            for index, field in enumerate(self.schema['fields']):
                rdata[field['name']] = row[index]
            cdata.append(rdata)

        ins = table.insert()
        conn = self.__engine.connect()
        conn.execute(ins, cdata)

    def get_data(self):
        """Return table's data.

        Returns
        -------
        generator
            Generator of data tuples.

        """

        metadata = MetaData()
        table = Table(self.__name, metadata,
                autoload=True, autoload_with=self.__engine,
                schema=self.__dbschema)

        conn = self.__engine.connect()
        result = conn.execute(select([table]))

        return list(result)

    # Private

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
        columns = []
        for field in schema['fields']:
            try:
                column_type = mapping[field['type']]
            except KeyError:
                message = 'Type %s is not supported' % field['type']
                raise TypeError(message)
            column = Column(field['name'], column_type)
            columns.append(column)

        return columns

    def __restore_schema(self, table):
        """Convert SQLAlchemy table reflection to JSONTableSchema schema.
        """

        # Mapping
        mapping = {
            TEXT: 'string',
            Integer: 'integer',
            Float: 'number',
            Boolean: 'boolean',
        }

        # Convert
        fields = []
        for column in table.columns:
            try:
                field_type = mapping[column.type.__class__]
            except KeyError:
                message = 'Type %s is not supported' % column.type
                raise TypeError(message)
            field = {'name': column.name, 'type': field_type}
            fields.append(field)
        schema = {'fields': fields}

        return schema
