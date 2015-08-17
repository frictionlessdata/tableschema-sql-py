import os
import json
import unicodecsv
from typecast import cast


fixtures = os.path.join(os.path.dirname(__file__), 'fixtures')


def load_fixture(name):
    path = os.path.join(fixtures, name)
    schema_path = path + '.json'
    with open(schema_path, 'r') as fh:
        schema = json.load(fh)
    data = []
    with open(path, 'r') as fh:
        for row in unicodecsv.DictReader(fh):
            conv = {}
            for field in schema.get('fields'):
                val = row.get(field['name'])
                conv[field['name']] = cast(field['type'], val)
            data.append(conv)
    return schema, data
