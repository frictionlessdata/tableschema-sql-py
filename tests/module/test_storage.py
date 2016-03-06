# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import unittest
from mock import MagicMock, patch, ANY
from importlib import import_module
module = import_module('jsontableschema_sql.storage')


class TestTable(unittest.TestCase):

    # Helpers

    def setUp(self):

        # Mocks
        self.addCleanup(patch.stopall)
        self.MetaData = patch.object(module, 'MetaData').start()
        self.metadata = self.MetaData.return_value
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
        dbtable1 = MagicMock()
        dbtable2 = MagicMock()
        dbtable1.name = 'prefix_table1'
        dbtable2.name = 'prefix_table2'
        self.metadata.sorted_tables = [dbtable1, dbtable2]

        # Assert values
        assert self.storage.tables == ['table1', 'table2']

    def test_check(self):

        # Mocks
        dbtable1 = MagicMock()
        dbtable2 = MagicMock()
        dbtable1.name = 'prefix_table1'
        dbtable2.name = 'prefix_table2'
        self.metadata.sorted_tables = [dbtable1, dbtable2]

        # Assert values
        assert self.storage.check('table1')
        assert self.storage.check('table2')
        assert not self.storage.check('table3')

    @unittest.skip('write')
    def test_create(self):
        pass

    def test_create_existent(self):

        # Mocks
        self.storage.check = MagicMock(return_value=True)

        # Assert raises
        with pytest.raises(RuntimeError):
            self.storage.create('table', 'schema')

    @unittest.skip('write')
    def test_delete(self):
        pass

    def test_delete_non_existent(self):

        # Assert raises
        with pytest.raises(RuntimeError):
            self.storage.delete('table')

    @unittest.skip('write')
    def test_describe(self):
        pass

    @unittest.skip('write')
    def test_read(self):
        pass

    @unittest.skip('write')
    def test_write(self):
        pass
