# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import json
import jsontableschema
from jsontableschema.exceptions import InvalidObjectType
from sqlalchemy import Table, MetaData
from . import mappers


# Module API

class Storage(object):
    """SQL Tabular Storage.

    It's an implementation of `jsontablescema.Storage`.

    Args:
        engine (object): SQLAlchemy engine
        dbschema (str): database schema name
        prefix (str): prefix for all buckets

    """

    # Public

    def __init__(self, engine, dbschema=None, prefix=''):

        # Set attributes
        self.__connection = engine.connect()
        self.__dbschema = dbschema
        self.__prefix = prefix
        self.__descriptors = {}

        # Create metadata
        self.__metadata = MetaData(
            bind=self.__connection,
            schema=self.__dbschema,
            reflect=True)

    def __repr__(self):

        # Template and format
        template = 'Storage <{engine}/{dbschema}>'
        text = template.format(
            engine=self.__connection.engine,
            dbschema=self.__dbschema)

        return text

    @property
    def buckets(self):

        # Collect
        buckets = []
        for table in self.__metadata.sorted_tables:
            bucket = mappers.tablename_to_bucket(self.__prefix, table.name)
            if bucket is not None:
                buckets.append(bucket)

        return buckets

    def create(self, bucket, descriptor, force=False):

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        descriptors = descriptor
        if isinstance(descriptor, dict):
            descriptors = [descriptor]

        # Check buckets for existence
        for bucket in reversed(self.buckets):
            if bucket in buckets:
                if not force:
                    message = 'Bucket "%s" already exists.' % bucket
                    raise RuntimeError(message)
                self.delete(bucket)

        # Define buckets
        for bucket, descriptor in zip(buckets, descriptors):

            # Add to schemas
            self.__descriptors[bucket] = descriptor

            # Crate table
            jsontableschema.validate(descriptor)
            tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
            columns, constraints = mappers.descriptor_to_columns_and_constraints(
                self.__prefix, bucket, descriptor)
            Table(tablename, self.__metadata, *(columns+constraints))

        # Create tables, update metadata
        self.__metadata.create_all()

    def delete(self, bucket=None, ignore=False):

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        elif bucket is None:
            buckets = reversed(self.buckets)

        # Iterate over buckets
        tables = []
        for bucket in buckets:

            # Check existent
            if bucket not in self.buckets:
                if not ignore:
                    message = 'Bucket "%s" doesn\'t exist.' % bucket
                    raise RuntimeError(message)

            # Remove from buckets
            if bucket in self.__descriptors:
                del self.__descriptors[bucket]

            # Add table to tables
            table = self.__get_table(bucket)
            tables.append(table)

        # Drop tables, update metadata
        self.__metadata.drop_all(tables=tables)
        self.__metadata.clear()
        self.__metadata.reflect()

    def describe(self, bucket, descriptor=None):

        # Set descriptor
        if descriptor is not None:
            self.__descriptors[bucket] = descriptor

        # Get descriptor
        else:
            descriptor = self.__descriptors.get(bucket)
            if descriptor is None:
                table = self.__get_table(bucket)
                descriptor = mappers.columns_and_constraints_to_descriptor(
                    self.__prefix, table.name, table.columns, table.constraints)

        return descriptor

    def iter(self, bucket):

        # Get result
        table = self.__get_table(bucket)
        # Streaming could be not working for some backends:
        # http://docs.sqlalchemy.org/en/latest/core/connections.html
        select = table.select().execution_options(stream_results=True)
        result = select.execute()

        # Yield data
        for row in result:
            yield list(row)

    def read(self, bucket):

        # Get rows
        rows = list(self.iter(bucket))

        return rows

    def write(self, bucket, rows):

        # Prepare
        BUFFER_SIZE = 1000
        descriptor = self.describe(bucket)
        schema = jsontableschema.Schema(descriptor)
        table = self.__get_table(bucket)

        # Write
        with self.__connection.begin():
            keyed_rows = []
            for row in rows:
                keyed_row = {}
                for index, field in enumerate(schema.fields):
                    value = row[index]
                    try:
                        value = field.cast_value(value)
                    except InvalidObjectType:
                        value = json.loads(value)
                    keyed_row[field.name] = value
                keyed_rows.append(keyed_row)
                if len(keyed_rows) > BUFFER_SIZE:
                    # Insert data
                    table.insert().execute(keyed_rows)
                    # Clean memory
                    keyed_rows = []
            if len(keyed_rows) > 0:
                # Insert data
                table.insert().execute(keyed_rows)

    # Private

    def __get_table(self, bucket):
        """Return SQLAlchemy table for the given bucket.
        """

        # Prepare name
        tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
        if self.__dbschema:
            tablename = '.'.join(self.__dbschema, tablename)

        return self.__metadata.tables[tablename]
