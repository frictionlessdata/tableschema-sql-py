# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from jsontableschema_sql import mappers


# Tests

def test_convert_table():
    assert mappers.convert_table('prefix_', 'table') == 'prefix_table'


def test_restore_table():
    assert mappers.restore_table('prefix_', 'prefix_table') == 'table'
    assert mappers.restore_table('prefix_', 'xxxxxx_table') == None


@pytest.mark.skip('write')
def test_convert_schema():
    pass

@pytest.mark.skip('write')
def test_restore_schema():
    pass
