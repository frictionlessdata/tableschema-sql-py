# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import unittest
from mock import MagicMock, patch, ANY
from importlib import import_module
module = import_module('jtssql.table')


class TestTable(unittest.TestCase):

    # Helpers

    def setUp(self):

        # Mocks
        self.addCleanup(patch.stopall)
        self.service = MagicMock()
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.table_id = 'table_id'

        # Create table
        self.table = module.Table(
                service=self.service,
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id)

    # Tests

    def test___repr__(self):

        # Assert values
        assert repr(self.table)

    def test_service(self):

        # Assert values
        assert self.table.service == self.service

    def test_project_id(self):

        # Assert values
        assert self.table.project_id == self.project_id

    def test_dataset_id(self):

        # Assert values
        assert self.table.dataset_id == self.dataset_id

    def test_table_id(self):

        # Assert values
        assert self.table.table_id == self.table_id

    def test_is_existent_true(self):

        # Assert values
        assert self.table.is_existent

    def test_is_existent_false(self):

        # Mocks
        error = Exception()
        error.resp = MagicMock(status=404)
        patch.object(module, 'HttpError', Exception).start()
        self.service.tables.side_effect = error

        # Assert values
        assert not self.table.is_existent

    def test_is_existent_raise(self):

        # Mocks
        error = Exception()
        error.resp = MagicMock(status=500)
        patch.object(module, 'HttpError', Exception).start()
        self.service.tables.side_effect = error

        # Assert exception
        with pytest.raises(module.HttpError):
           self.table.is_existent

    def test_create_existent(self):

        # Assert exception
        with pytest.raises(RuntimeError):
           self.table.create('schema')

    def test_create(self):

        # Mocks
        patch.object(self.table.__class__, 'is_existent', False).start()

        # Method call
        self.table.create('schema')

        # Assert calls
        # TODO: add body check
        self.service.tables.return_value.insert.assert_called_with(
                projectId=self.project_id,
                datasetId=self.dataset_id,
                body=ANY)

    def test_delete_non_existent(self):

        # Mocks
        patch.object(self.table.__class__, 'is_existent', False).start()

        # Assert exception
        with pytest.raises(RuntimeError):
           self.table.delete()

    def test_delete(self):

        # Method call
        self.table.delete()

        # Assert values
        self.table.schema is None

        # Assert calls
        self.service.tables.return_value.delete.assert_called_with(
                projectId=self.project_id,
                datasetId=self.dataset_id,
                tableId=self.table_id)

    def test_schema(self):

        # Mocks
        tables = self.service.tables.return_value

        # Assert values
        assert self.table.schema == tables.get.return_value.execute.return_value['schema']

        # Assert calls
        tables.get.assert_called_with(
                projectId=self.project_id,
                datasetId=self.dataset_id,
                tableId=self.table_id)

    def test_add_data(self):
        pass

    def test_get_data(self):
        pass
