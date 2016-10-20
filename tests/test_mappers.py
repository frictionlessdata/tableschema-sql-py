# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from mock import Mock
from jsontableschema_sql import mappers


# Tests

def test_bucket_to_tablename():
    assert mappers.bucket_to_tablename('prefix_', 'bucket') == 'prefix_bucket'


def test_tablename_to_bucket():
    assert mappers.tablename_to_bucket('prefix_', 'prefix_bucket') == 'bucket'
    assert mappers.tablename_to_bucket('prefix_', 'xxxxxx_bucket') == None


def test_descriptor_to_columns_and_constraints_not_supported_type():
    descriptor = {
        'fields': [{'name': 'name', 'type': 'not_supported'}],
    }
    with pytest.raises(TypeError):
        mappers.descriptor_to_columns_and_constraints(
            'prefix_', 'bucket', descriptor)


def test_descriptor_to_columns_and_constraints_not_supported_reference():
    descriptor = {
        'foreignKeys': [{'fields': '', 'reference': {'resource': 'not_supported'}}],
        'fields': [],
    }
    with pytest.raises(ValueError):
        mappers.descriptor_to_columns_and_constraints(
            'prefix_', 'bucket', descriptor)


def test_columns_and_constraints_to_descriptor_not_supported_type():
    with pytest.raises(TypeError):
        mappers.columns_and_constraints_to_descriptor(
            'prefix_', 'tablename', [Mock()], [])
