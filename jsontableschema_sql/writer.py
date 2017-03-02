# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import json

import pybloom_live
from sqlalchemy import select

import jsontableschema
from jsontableschema.exceptions import InvalidObjectType


BUFFER_SIZE = 1000


class StorageWriter(object):

    def __init__(self, table, descriptor, update_keys):

        self.table = table
        self.descriptor = descriptor
        self.update_keys = update_keys
        if update_keys is not None:
            self.__prepare_bloom()
        self.__buffer = []

    def write(self, rows, keyed):
        # Prepare
        schema = jsontableschema.Schema(self.descriptor)

        # Write
        for row in rows:
            if not keyed:
                row = self.__convert_to_keyed(schema, row)

            keyed_row = row

            if self.__check_existing(keyed_row):
                self.__insert()
                if self.__update(row):
                    continue

            self.__buffer.append(keyed_row)

            if len(self.__buffer) > BUFFER_SIZE:
                self.__insert()
            yield keyed_row

        self.__insert()

    def __insert(self):
        if len(self.__buffer) > 0:
            # Insert data
            self.table.insert().execute(self.__buffer)
            # Clean memory
            self.__buffer = []

    def __update(self, row):
        expr = self.table.update().values(row)
        for key in self.update_keys:
            expr = expr.where(getattr(self.table.c, key) == row[key])
        res = expr.execute()
        return res.rowcount > 0

    @staticmethod
    def __convert_to_keyed(schema, row):
        keyed_row = {}
        for index, field in enumerate(schema.fields):
            value = row[index]
            try:
                value = field.cast_value(value)
            except InvalidObjectType:
                value = json.loads(value)
            keyed_row[field.name] = value

        return keyed_row

    def __prepare_bloom(self):
        self.bloom = pybloom_live.ScalableBloomFilter()
        columns = [getattr(self.table.c, key) for key in self.update_keys]
        keys = select(columns).execution_options(stream_results=True).execute()
        for key in keys:
            self.bloom.add(key)

    def __check_existing(self, row):
        if self.update_keys is not None:
            key = tuple(row[key] for key in self.update_keys)
            if key in self.bloom:
                return True
            else:
                self.bloom.add(key)
                return False
        else:
            return False
