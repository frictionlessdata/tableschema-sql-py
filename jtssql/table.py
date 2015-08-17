import json

from sqlalchemy import MetaData
from sqlalchemy.schema import Table, Column
from sqlalchemy.types import Unicode

# from spendb.core import db
# from spendb.model.common import json_default
# from spendb.validation.model import TYPES


class SchemaTable(object):
    """ The ``FactTable`` serves as a controller object for
    a given ``Model``, handling the creation, filling and migration
    of the table schema associated with the dataset. """

    def __init__(self, dataset):
        self.dataset = dataset
        self.bind = db.engine
        self.table_name = '%s__facts' % dataset.name
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
    def alias(self):
        """ An alias used for queries. """
        if not hasattr(self, '_alias'):
            self._alias = self.table.alias('entry')
        return self._alias

    @property
    def mapping(self):
        if not hasattr(self, '_mapping'):
            self._mapping = {}
            for attribute in self.dataset.model.attributes:
                if attribute.column in self.alias.columns:
                    col = self.alias.c[attribute.column]
                    self._mapping[attribute.path] = col
        return self._mapping

    @property
    def exists(self):
        return db.engine.has_table(self.table.name)

    def _fields_columns(self, table):
        """ Transform the (auto-detected) fields into a set of column
        specifications. """
        for field in self.dataset.fields:
            data_type = TYPES.get(field.get('type'), Unicode)
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
        """ Create the fact table if it does not exist. """
        if not self.exists:
            self.table.create(self.bind)

    def drop(self):
        """ Drop the fact table if it does exist. """
        if self.exists:
            self.table.drop()
        self._table = None

    def __repr__(self):
        return "<FactTable(%r)>" % (self.dataset)
