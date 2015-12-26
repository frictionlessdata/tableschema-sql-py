# jsontableschema-bigquery-py

[![Travis](https://img.shields.io/travis/okfn/jsontableschema-bigquery-py.svg)](https://travis-ci.org/okfn/jsontableschema-bigquery-py)
[![Coveralls](http://img.shields.io/coveralls/okfn/jsontableschema-bigquery-py.svg?branch=master)](https://coveralls.io/r/okfn/jsontableschema-bigquery-py?branch=master)

Generate and load BigQuery tables based on JSON Table Schema descriptors.

## Usage

This section is intended to be used by end-users of the library.

### Resource and Table

> See section below how to get authentificated service.

Resource represents Big Query table wrapped in JSON Table Schema
converters and validators:

```python
from jtsbq import Resource

resource = Resource(<service>, 'project_id', 'dataset_id', 'table_id')

resource.create(schema)
resource.schema

resource.add_data(data)
resource.get_data()

resource.import_data(path)
resource.export_schema(path)
resource.export_data(path)
```

Table represents native Big Query table:

```python
from jtsbq import Table

table = Table(<service>, 'project_id', 'dataset_id', 'table_id')

table.create(schema)
table.schema

table.add_data(data)
table.get_data()
```

### Authentificated service

To start using Google BigQuery service:
- Create a new project - [link](https://console.developers.google.com/home/dashboard)
- Create a service key - [link](https://console.developers.google.com/apis/credentials)
- Download json credentials and set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

For example:

```python
import os
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.credentials.json'
credentials = GoogleCredentials.get_application_default()
service = build('bigquery', 'v2', credentials=credentials)
```

### Design Overview

#### Entities

- Table

    Table is a native BigQuery table. Schema and data are used as it is in BigQuery.
    For example to create a Table user has to pass a BigQuery compatible schema.

- Resource

    Resource is a Table wrapperd in JSONTableSchema converters and validators.
    That means user interacts with Resource in JSONTableSchema terms. For example
    to create an underlaying Table user has to pass JSONTableSchema compatible schema.
    All data are returned after JSONTableSchema conversion.

> Resource is a JSONTableSchema facade to Table (BigQuery) backend.

Table and Resource are geteways by their nature. It means user can initiate
Table without real BigQuery table creation then call `create` or `delete` to
delete the real table without instance destruction.

#### Mappings

```
schema.json -> BigQuery table schema
data.csv -> BigQuery talbe data
```

#### Drivers

Default Google BigQuery client is used - [docs](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/).

### Documentation

API documentation is presented as docstings:
- [Resource](https://github.com/okfn/jsontableschema-bigquery-py/blob/master/jtsbq/resource.py)
- [Table](https://github.com/okfn/jsontableschema-bigquery-py/blob/master/jtsbq/table.py)

## Development

This section is intended to be used by tech users collaborating
on this project.

### Getting Started

To activate virtual environment, install
dependencies, add pre-commit hook to review and test code
and get `run` command as unified developer interface:

```
$ source activate.sh
```

### Reviewing

The project follow the next style guides:
- [Open Knowledge Coding Standards and Style Guide](https://github.com/okfn/coding-standards)
- [PEP 8 - Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/)

To check the project against Python style guide:

```
$ run review
```

### Testing

To run tests with coverage check:

```
$ run test
```

Coverage data will be in the `.coverage` file.
