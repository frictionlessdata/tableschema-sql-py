from jtssql.table import SchemaTable
from jtssql.util import JTS_TYPES

__all__ = [SchemaTable, JTS_TYPES]
__version__ = '0.1'


def make_table(engine, table_name, schema):
    """ Generate a table (named ``table_name``) against the database connection
    managed by the given ``engine``. The table will have the columns specified
    in the field spec of the given JTS ``schema``. """
    table = SchemaTable(engine, table_name, schema)
    table.create()
    return table.table
