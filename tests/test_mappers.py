# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from mock import Mock

from jsontableschema_sql import mappers


# Tests

def test_convert_table():
    assert mappers.convert_table('prefix_', 'table') == 'prefix_table'


def test_restore_table():
    assert mappers.restore_table('prefix_', 'prefix_table') == 'table'
    assert mappers.restore_table('prefix_', 'xxxxxx_table') == None


def test_convert_schema_not_supported_type():
    with pytest.raises(TypeError):
        mappers.convert_schema('prefix_', 'table', {
            'fields': [{'type': 'not_supported'}]})


def test_convert_schema_not_supported_reference():
    with pytest.raises(ValueError):
        mappers.convert_schema('prefix_', 'table', {
            'foreignKeys': [{'fields': '', 'reference': {'resource': 'not_supported'}}],
            'fields': []})


def test_restore_schema_not_supported_tupe():
    with pytest.raises(TypeError):
        mappers.restore_schema('prefix_', 'table', [Mock()], [])
