# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import csv
import six
import json
from tabulator import topen, processors
from jsontableschema.model import SchemaModel

from . import schema as schema_module
from .table import Table


# Module API

class Resource(object):
    """Data resource stored on BigQuery gateway.

    Parameters
    ----------
    service: object
        Authentificated BigQuery service.
    project_id: str
        BigQuery project identifier.
    dataset_id: str
        BigQuery dataset identifier.
    table_id: str
        BigQuery table identifier.

    """

    # Public

    def __init__(self, service, project_id, dataset_id, table_id):

        # Initiate table
        self.__table = Table(
                service=service,
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id)

    def __repr__(self):

        # Template and format
        template = 'Resource <table: {table}>'
        text = template.format(table=self.__table)

        return text

    @property
    def table(self):
        """Return underlaying table.
        """

        return self.__table

    @property
    def is_existent(self):
        """Return if resource (underlaying table) is existent.
        """

        return self.__table.is_existent

    def create(self, schema):
        """Create resource by JSON Table schema.

        Raises
        ------
        RuntimeError
            If resource (underlaying table) is already existent.

        """

        # Convert schema
        model = SchemaModel(schema)
        schema = schema_module.resource2table({'fields': model.fields})

        # Create table
        self.__table.create(schema)

    def delete(self):
        """Delete resource (underlaying table).

        Raises
        ------
        RuntimeError
            If resource (underlaying table) is not existent.

        """

        # Delete table
        self.__table.delete()

    @property
    def schema(self):
        """Return JSONTableSchema dict.
        """

        # Create cache
        if not hasattr(self, '__schema'):

            # Get and convert schema
            schema = self.__table.schema
            schema = schema_module.table2resource(schema)
            self.__schema = schema

        return self.__schema

    def add_data(self, data):
        """Add data to resource.

        Parameters
        ----------
        data: list
            List of data tuples.

        """

        # Get model and data
        model = SchemaModel(self.schema)
        cdata = []
        for row in data:
            row = tuple(model.convert_row(*row))
            cdata.append(row)

        # Add data to table
        self.__table.add_data(cdata)

    def get_data(self):
        """Return data generator.

        Returns
        -------
        generator
            Generator of data tuples.
        """

        # Get model and data
        model = SchemaModel(self.schema)
        data = self.__table.get_data()

        # Yield converted data
        for row in data:
            row = tuple(model.convert_row(*row))
            yield row

    def import_data(self, path, **options):
        """Import data from file.

        Parameters
        ----------
        path: str
            Tabulator compatible path to data.
        options: dict
            Tabulator options.

        """

        # Get data
        data = []
        with topen(path, with_headers=True, **options) as table:
            table.add_processor(processors.Schema(self.schema))
            for row in table:
                data.append(row)

        # Add data to table
        self.__table.add_data(data)

    def export_schema(self, path):
        """Export schema to file.

        Parameters
        ----------
        path: str
            Path where to store schema file.

        """

        # Ensure directory
        self.__ensure_dir(path)

        # Write dump on disk
        with io.open(path,
                     mode=self.__write_mode,
                     encoding=self.__write_encoding) as file:
            json.dump(self.schema, file, indent=4)

    def export_data(self, path):
        """Export data to file.

        Parameters
        ----------
        path: str
            Path where to store data file.
        """

        # Get model
        model = SchemaModel(self.schema)

        # Ensure directory
        self.__ensure_dir(path)

        # Write csv on disk
        with io.open(path,
                     mode=self.__write_mode,
                     newline=self.__write_newline,
                     encoding=self.__write_encoding) as file:
            writer = csv.writer(file)
            writer.writerow(model.headers)
            for row in self.get_data():
                writer.writerow(row)

    # Private

    def __ensure_dir(self, path):
        dirpath = os.path.dirname(path)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

    @property
    def __write_mode(self):
        if six.PY2:
            return 'wb'
        return 'w'

    @property
    def __write_encoding(self):
        if six.PY2:
            return None
        return 'utf-8'

    @property
    def __write_newline(self):
        if six.PY2:
            return None
        return ''
