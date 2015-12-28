# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals


# Module API

def import_resource(storage, place, schema_path, data_path):

    # Get data
    data = []
    with topen(path, with_headers=True, **options) as table:
        table.add_processor(processors.Schema(self.schema))
        for row in table:
            data.append(row)

    # Add data to table
    self.__table.add_data(data)


def export_resource(storage, place, schema_path, data_path):

    # Ensure schema and data dirs
    for path in [schema_path, data_path]:
        dirpath = os.path.dirname(path)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

    # Write schema on disk
    with io.open(path,
                 mode=self.__write_mode,
                 encoding=self.__write_encoding) as file:
        json.dump(self.schema, file, indent=4)

    # Write data on disk
    with io.open(path,
                 mode=self.__write_mode,
                 newline=self.__write_newline,
                 encoding=self.__write_encoding) as file:
        writer = csv.writer(file)
        writer.writerow(model.headers)
        for row in self.get_data():
            writer.writerow(row)
