# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import json
import jsontableschema
from jsontableschema import storage as base
from jsontableschema.model import SchemaModel
from jsontableschema.exceptions import InvalidObjectType
from sqlalchemy import Table, MetaData

from . import mappers


# Module API

class Storage(base.Storage):
    """SQL Tabular Storage.

    Parameters
    ----------
    engine: object
        SQLAlchemy engine.
    dbschema: str
        Database schema name.
    prefix: str
        Prefix for all tables.

    """

    # Public

    def __init__(self, engine, dbschema=None, prefix=''):

        # Set attributes
        self.__engine = engine
        self.__dbschema = dbschema
        self.__prefix = prefix
        self.__schemas = {}

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
        """Return list of storage's table names.
        """

        # Collect
        tables = []
        for dbtable in self.__metadata.sorted_tables:
            table = dbtable.name
            table = mappers.restore_table(self.__prefix, table)
            if table is not None:
                tables.append(table)

        return tables

    def check(self, table):
        """Return if table exists.
        """

        # Check existence
        existence = table in self.tables

        return existence

    def create(self, table, schema):
        """Create table by schema.

        Parameters
        ----------
        table: str/list
            Table name or list of table names.
        schema: dict/list
            JSONTableSchema schema or list of schemas.

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

        # Check tables for existence
        for table in tables:
            if self.check(table):
                message = 'Table "%s" already exists.' % table
                raise RuntimeError(message)

        # Define tables
        for table, schema in zip(tables, schemas):

            # Add to schemas
            self.__schemas[table] = schema

            # Crate sa table
            table = mappers.convert_table(self.__prefix, table)
            jsontableschema.validate(schema)
            columns, constraints = mappers.convert_schema(
                    self.__prefix, table, schema)
            Table(table, self.__metadata, *(columns+constraints))

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
            If table doesn't exist.

        """

        # Make lists
        tables = table
        if isinstance(table, six.string_types):
            tables = [table]

        # Iterate over tables
        dbtables = []
        for table in tables:

            # Check existent
            if not self.check(table):
                message = 'Table "%s" doesn\'t exist.' % self
                raise RuntimeError(message)

            # Remove from schemas
            if table in self.__schemas:
                del self.__schemas[table]

            # Add table to dbtables
            dbtable = self.__get_dbtable(table)
            dbtables.append(dbtable)

        # Drop tables, update metadata
        self.__metadata.drop_all(tables=dbtables)
        self.__metadata.clear()
        self.__metadata.reflect()

    def describe(self, table):
        """Return table's JSONTableSchema schema.

        Parameters
        ----------
        table: str
            Table name.

        Returns
        -------
        dict
            JSONTableSchema schema.

        """

        # Get schema
        if table in self.__schemas:
            schema = self.__schemas[table]
        else:
            dbtable = self.__get_dbtable(table)
            table = mappers.convert_table(self.__prefix, table)
            schema = mappers.restore_schema(
                    self.__prefix, table, dbtable.columns, dbtable.constraints)

        return schema

    def read(self, table):
        """Read data from table.

        Parameters
        ----------
        table: str
            Table name.

        Returns
        -------
        generator
            Data tuples generator.

        """

        # Get result
        dbtable = self.__get_dbtable(table)
        # Streaming could be not working for some backends:
        # http://docs.sqlalchemy.org/en/latest/core/connections.html
        select = dbtable.select().execution_options(stream_results=True)
        result = select.execute()

        # Yield data
        for row in result:
            yield row

    def write(self, table, data):
        """Write data to table.

        Parameters
        ----------
        table: str
            Table name.
        data: list
            List of data tuples.

        """

        # Process data
        schema = self.describe(table)
        model = SchemaModel(schema)
        dbtable = self.__get_dbtable(table)
        rows = []
        for row in data:
            row_dict = {}
            for index, field in enumerate(model.fields):
                value = row[index]
                try:
                    value = model.cast(field['name'], value)
                except InvalidObjectType as exception:
                    value = json.loads(value)
                row_dict[field['name']] = value
            rows.append(row_dict)
            if len(rows) > 1000:
                # Insert data
                dbtable.insert().execute(rows)
                # Clean memory
                rows = []
        if len(rows) > 0:
            # Insert data
            dbtable.insert().execute(rows)

    # Private

    def __get_dbtable(self, table):
        """Return dbtable instance from metadata.
        """

        # Prepare dict key
        key = mappers.convert_table(self.__prefix, table)
        if self.__dbschema:
            # TODO: Start to test dbschema parameter
            key = '.'.join(self.__dbschema, key)  # pragma: no cover

        return self.__metadata.tables[key]
