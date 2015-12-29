# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import unittest
from mock import MagicMock, patch, ANY
from importlib import import_module
module = import_module('jtssql.storage')


class TestTable(unittest.TestCase):

    # Helpers

    def setUp(self):

        # Mocks
        self.addCleanup(patch.stopall)
        self.engine = MagicMock()
        self.dbschema = 'dbschema'
        self.prefix = 'prefix_'

        # Create table
        self.storage = module.Storage(
                engine=self.engine,
                dbschema=self.dbschema,
                prefix=self.prefix)

    # Tests

    def test___repr__(self):
        assert repr(self.storage)

    def test_tables(self):

        # Mocks
        self.engine.table_names.return_value = ['prefix_table1', 'prefix_table2']

        # Assert values
        assert self.storage.tables == ['table1', 'table2']

    def test_check(self):

        # Mocks
        self.engine.table_names.return_value = ['prefix_table1', 'prefix_table2']

        # Assert values
        assert self.storage.check('table1')
        assert self.storage.check('table2')
        assert not self.storage.check('table3')

    def test_create(self):
        pass

    def test_create_existent(self):

        # Mocks
        self.storage.check = MagicMock(return_value=True)

        # Assert raises
        with pytest.raises(RuntimeError):
            self.storage.create('table', 'schema')

    def test_delete(self):

        # Mocks
        self.storage.check = MagicMock(return_value=True)
        Table = patch.object(module, 'Table').start()

        # Method call
        self.storage.delete('table')

        # Assert calls
        Table.assert_called_with('prefix_table', ANY, schema=self.dbschema)
        Table.return_value.drop.assert_called_with(self.engine)

    def test_delete_non_existent(self):

        # Assert raises
        with pytest.raises(RuntimeError):
            self.storage.delete('table')

    def test_describe(self):
        pass

    def test_read(self):
        pass

    def test_write(self):
        pass
