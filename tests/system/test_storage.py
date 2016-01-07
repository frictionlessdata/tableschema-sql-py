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
from examples import spending


class TestStorage(unittest.TestCase):

    # Tests

    def test_spending(self):

        # Run function
        tables, schema, data = base.run(
                spending.url, spending.prefix, spending.table,
                spending.schema, spending.data)

        # Assert values
        assert spending.table in tables
        assert schema == spending.schema
        assert data == spending.data
