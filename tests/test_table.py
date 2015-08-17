from unittest import TestCase
from sqlalchemy import create_engine

from jtssql import SchemaTable, make_table

from .util import load_fixture


class SchemaTableTestCase(TestCase):

    def setUp(self):
        super(SchemaTableTestCase, self).setUp()
        self.engine = create_engine('sqlite://')

    def test_api(self):
        schema, data = load_fixture('sa.csv')
        table = SchemaTable(self.engine, 'test', schema)
        assert 'test' in repr(table)

    def test_create(self):
        schema, data = load_fixture('sa.csv')
        table = SchemaTable(self.engine, 'test', schema)
        assert not table.exists, table
        table.create()
        assert table.exists, table
        assert '_id' in table.table.columns
        table.drop()
        assert not table.exists, table

    def test_make_table(self):
        schema, data = load_fixture('countries.csv')
        table = make_table(self.engine, 'test', schema)
        assert '_id' in table.columns

    def test_load_data(self):
        schema, data = load_fixture('sa.csv')
        table = SchemaTable(self.engine, 'test', schema)
        table.create()
        table.load_iter(data)
        res = self.engine.execute(table.table.select())
        res = list(res)
        assert len(res) == len(data), res

    def test_load_other(self):
        schema, data = load_fixture('countries.csv')
        table = SchemaTable(self.engine, 'test', schema)
        table.create()
        table.load_iter(data, chunk_size=2)
        res = self.engine.execute(table.table.select())
        res = list(res)
        assert len(res) == len(data), res
