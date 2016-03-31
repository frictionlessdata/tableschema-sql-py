# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import json
from tabulator import topen
from sqlalchemy import create_engine
from dotenv import load_dotenv; load_dotenv('.env')

from jsontableschema_sql import Storage


# Get resources
articles_schema = json.load(io.open('data/articles.json', encoding='utf-8'))
comments_schema = json.load(io.open('data/comments.json', encoding='utf-8'))
articles_data = topen('data/articles.csv', with_headers=True).read()
comments_data = topen('data/comments.csv', with_headers=True).read()

# Engine
engine = create_engine(os.environ['DATABASE_URL'])

# Storage
storage = Storage(engine=engine, prefix='prefix_')

# Delete tables
for table in reversed(storage.tables):
    storage.delete(table)

# Create tables
storage.create(['articles', 'comments'], [articles_schema, comments_schema])

# Write data to tables
storage.write('articles', articles_data)
storage.write('comments', comments_data)

# List tables
print(storage.tables)

# Describe tables
print(storage.describe('articles'))
print(storage.describe('comments'))

# Read data from tables
print(list(storage.read('articles')))
print(list(storage.read('comments')))
