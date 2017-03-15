# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import collections
import jsontableschema
from sqlalchemy import Table, MetaData
from . import mappers
from .writer import StorageWriter


# Module API

class Storage(object):
    """SQL Tabular Storage.

    It's an implementation of `jsontablescema.Storage`.

    Args:
        engine (object): SQLAlchemy engine
        dbschema (str): database schema name
        prefix (str): prefix for all buckets
        reflect_only (callable): a boolean predicate to filter
            the list of table names when reflecting
    """

    # Public

    def __init__(self, engine, dbschema=None, prefix='', reflect_only=None,
                 autoincrement=None):

        # Set attributes
        self.__connection = engine.connect()
        self.__dbschema = dbschema
        self.__prefix = prefix
        self.__descriptors = {}
        self.__autoincrement = autoincrement
        if reflect_only is not None:
            self.__only = reflect_only
        else:
            self.__only = lambda _: True

        # Create metadata
        self.__metadata = MetaData(
            bind=self.__connection,
            schema=self.__dbschema)
        self.__reflect()

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

    def create(self, bucket, descriptor, force=False, indexes_fields=None):
        """Create table by schema.

        Parameters
        ----------
        table: str/list
            Table name or list of table names.
        schema: dict/list
            JSONTableSchema schema or list of schemas.
        indexes_fields: list
            list of tuples containing field names, or list of such lists

        Raises
        ------
        RuntimeError
            If table already exists.

        """

        # Make lists
        buckets = bucket
        if isinstance(bucket, six.string_types):
            buckets = [bucket]
        descriptors = descriptor
        if isinstance(descriptor, dict):
            descriptors = [descriptor]
        if indexes_fields is None or len(indexes_fields) == 0:
            indexes_fields = [()] * len(descriptors)
        elif type(indexes_fields[0][0]) not in {list, tuple}:
            indexes_fields = [indexes_fields]
        assert len(indexes_fields) == len(descriptors)
        assert len(buckets) == len(descriptors)

        # Check buckets for existence
        for bucket in reversed(self.buckets):
            if bucket in buckets:
                if not force:
                    message = 'Bucket "%s" already exists.' % bucket
                    raise RuntimeError(message)
                self.delete(bucket)

        # Define buckets
        for bucket, descriptor, index_fields in zip(buckets, descriptors, indexes_fields):

            # Add to schemas
            self.__descriptors[bucket] = descriptor

            # Create table
            jsontableschema.validate(descriptor)
            tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
            columns, constraints, indexes = mappers.descriptor_to_columns_and_constraints(
                self.__prefix, bucket, descriptor, index_fields, self.__autoincrement)
            Table(tablename, self.__metadata, *(columns+constraints+indexes))

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
        self.__reflect()

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
                    self.__prefix, table.name, table.columns, table.constraints,
                    self.__autoincrement)

        return descriptor

    def iter(self, bucket):

        # Get result
        table = self.__get_table(bucket)

        # Make sure we close the transaction after iterating,
        #   otherwise it is left hanging
        with self.__connection.begin():
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

    def write(self, bucket, rows, keyed=False, as_generator=False, update_keys=None):

        if update_keys is not None and len(update_keys) == 0:
            raise ValueError('update_keys cannot be an empty list')

        table = self.__get_table(bucket)
        descriptor = self.describe(bucket)

        writer = StorageWriter(table, descriptor, update_keys, self.__autoincrement)

        with self.__connection.begin():
            gen = writer.write(rows, keyed)
            if as_generator:
                return gen
            else:
                collections.deque(gen, maxlen=0)

    # Private
    def __get_table(self, bucket):
        """Return SQLAlchemy table for the given bucket.
        """

        # Prepare name
        tablename = mappers.bucket_to_tablename(self.__prefix, bucket)
        if self.__dbschema:
            tablename = '.'.join((self.__dbschema, tablename))

        return self.__metadata.tables[tablename]

    def __reflect(self):
        def only(name, _):
            ret = (
                self.__only(name) and
                mappers.tablename_to_bucket(self.__prefix, name) is not None
            )
            return ret

        self.__metadata.reflect(only=only)
