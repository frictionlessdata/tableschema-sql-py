import json

from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from sqlalchemy.types import Unicode

from jtssql.util import JTS_TYPES, json_default


class SchemaTable(object):
    """ A SchemaTable uses the given JSON Table Schema to reflect, and, if
    necessary, generate a database table within the specified database. """

    def __init__(self, engine, table_name, schema):
        self.bind = engine
        self.table_name = table_name
        self.schema = schema
        self.meta = MetaData()
        self.meta.bind = self.bind
        self._table = None

    @property
    def table(self):
        """ Generate an appropriate table representation to mirror the
        fields known for this table. """
        if self._table is None:
            self._table = Table(self.table_name, self.meta)
            id_col = Column('_id', Unicode(42), primary_key=True)
            self._table.append_column(id_col)
            json_col = Column('_json', Unicode())
            self._table.append_column(json_col)
            self._fields_columns(self._table)
        return self._table

    @property
    def exists(self):
        return self.bind.has_table(self.table.name)

    def _fields_columns(self, table):
        """ Transform the (auto-detected) fields into a set of column
        specifications. """
        for field in self.schema.get('fields'):
            data_type = JTS_TYPES.get(field.get('type'), Unicode)
            col = Column(field.get('name'), data_type, nullable=True)
            table.append_column(col)

    def load_iter(self, iterable, chunk_size=1000):
        """ Bulk load all the data in an artifact to a matching database
        table. """
        chunk = []

        conn = self.bind.connect()
        tx = conn.begin()
        try:
            for i, record in enumerate(iterable):
                record['_id'] = i
                record['_json'] = json.dumps(record, default=json_default)
                chunk.append(record)
                if len(chunk) >= chunk_size:
                    stmt = self.table.insert()
                    conn.execute(stmt, chunk)
                    chunk = []

            if len(chunk):
                stmt = self.table.insert()
                conn.execute(stmt, chunk)
            tx.commit()
        except:
            tx.rollback()
            raise

    def create(self):
        """ Create the table if it does not exist. """
        if not self.exists:
            self.table.create(self.bind)

    def drop(self):
        """ Drop the table if it does exist. """
        if self.exists:
            self.table.drop()
        self._table = None

    def __repr__(self):
        return "<SchemaTable(%r)>" % (self.table_name)
