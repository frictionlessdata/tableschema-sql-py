# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import unittest
from importlib import import_module
module = import_module('jtssql.schema')


class schemaTest(unittest.TestCase):

    # Tests

    def test_t2r_not_supported_type(self):

        # Assert exception
        with pytest.raises(TypeError):
            module.table2resource({'fields': [{'type': 'not-supported'}]})

    def test_r2t_not_supported_type(self):

        # Assert exception
        with pytest.raises(TypeError):
            module.resource2table({'fields': [{'type': 'not-supported'}]})
