# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from mock import Mock
from tableschema_sql.mapper import Mapper


# Tests

def test_mapper_convert_bucket():
    mapper = Mapper('prefix_')
    assert mapper.convert_bucket('bucket') == 'prefix_bucket'


def test_mapper_convert_descriptor_not_supported_type():
    mapper = Mapper('prefix_')
    descriptor = {'fields': [{'name': 'name', 'type': 'not_supported'}]}
    with pytest.raises(TypeError):
        mapper.convert_descriptor('bucket', descriptor, [])


def test_mapper_restore_bucket():
    mapper = Mapper('prefix_')
    assert mapper.restore_bucket('prefix_bucket') == 'bucket'
    assert mapper.restore_bucket('xxxxxx_bucket') is None


def test_mapper_restore_descriptor_not_supported_type():
    mapper = Mapper('prefix_')
    with pytest.raises(TypeError):
        mapper.restore_descriptor('tablename', [Mock()], [])
