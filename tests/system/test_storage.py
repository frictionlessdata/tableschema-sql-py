# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import six
import sys
import json
import runpy
import tempfile
import unittest

from examples import base
from examples import testing


class TestStorage(unittest.TestCase):

    # Tests

    def test_testing(self):

        # Run function
        tables, schema, data = base.run(
                testing.url, testing.prefix, testing.table,
                testing.schema, testing.data)

        # Assert values
        assert testing.table in tables
        assert schema == testing.schema
        assert data == testing.data
