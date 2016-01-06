# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import pytest
import unittest
from mock import MagicMock, patch, mock_open, call, ANY
from importlib import import_module
module = import_module('jtssql.resource')


class Test_import_resource(unittest.TestCase):

    # Helpers

    def setUp(self):
        self.addCleanup(patch.stopall)
        self.topen = patch.object(module, 'topen').start()
        self.SchemaModel = patch.object(module, 'SchemaModel').start()
        self.storage = MagicMock()

    # Tests

    def test(self):

        # Call function
        module.import_resource(self.storage, 'table', 'schema', 'data')

        # Assert calls
        self.SchemaModel.assert_called_with('schema')
        self.storage.check.assert_called_with('table')
        self.storage.create.assert_called_with(
                'table', self.SchemaModel.return_value.as_python)
        self.topen.assert_called_with('data', with_headers=True)
        self.storage.write.assert_called_with(
                'table', self.topen.return_value.__enter__.return_value)


class Test_export_resource(unittest.TestCase):

    # Helpers

    def setUp(self):
        self.addCleanup(patch.stopall)
        self.ensure_dir = patch.object(module, '_ensure_dir').start()
        self.SchemaModel = patch.object(module, 'SchemaModel').start()
        self.storage = MagicMock()

    # Tests

    @unittest.skip('write')
    def test(self):
        pass
