# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import csv
import time
from apiclient.http import MediaIoBaseUpload
from apiclient.errors import HttpError


# Module API

class Table(object):
    """BigQuery native table gateway.

    Parameters
    ----------
    service: object
        Authentificated BigQuery service.
    project_id: str
        BigQuery project identifier.
    dataset_id: str
        BigQuery dataset identifier.
    table_id: str
        BigQuery table identifier.

    """

    # Public

    def __init__(self, service, project_id, dataset_id, table_id):

        # Set attributes
        self.__service = service
        self.__project_id = project_id
        self.__dataset_id = dataset_id
        self.__table_id = table_id
        self.__schema = None

    def __repr__(self):

        # Template
        template = 'Table <{project_id}:{dataset_id}.{table_id}>'

        # Format
        text = template.format(
                project_id=self.__project_id,
                dataset_id=self.__dataset_id,
                table_id=self.__table_id)

        return text

    @property
    def service(self):
        """Return BigQuery service instance.
        """

        return self.__service

    @property
    def project_id(self):
        """Return BigQuery project identifier.
        """

        return self.__project_id

    @property
    def dataset_id(self):
        """Return BigQuery dataset identifier.
        """

        return self.__dataset_id

    @property
    def table_id(self):
        """Return BigQuery table identifier.
        """

        return self.__table_id

    @property
    def is_existent(self):
        """Return table if is existent.
        """

        # If schema
        try:
            self.schema
            return True

        # No schema
        except HttpError as error:
            if error.resp.status != 404:
                raise
            return False

    def create(self, schema):
        """Create table by schema.

        Parameters
        ----------
        schema: dict
            BigQuery schema descriptor.

        Raises
        ------
        RuntimeError
            If table is already existent.

        """

        # Check not existent
        if self.is_existent:
            message = 'Table "%s" is already existent.' % self
            raise RuntimeError(message)

        # Prepare job body
        body = {
            'tableReference': {
                'projectId': self.__project_id,
                'datasetId': self.__dataset_id,
                'tableId': self.__table_id,
            },
            'schema': schema,
        }

        # Make request
        self.__service.tables().insert(
                projectId=self.__project_id,
                datasetId=self.__dataset_id,
                body=body).execute()

    def delete(self):
        """Delete table.

        Raises
        ------
        RuntimeError
            If table is not existent.

        """

        # Check existent
        if not self.is_existent:
            message = 'Table "%s" is not existent.' % self
            raise RuntimeError(message)

        # Make request
        self.__service.tables().delete(
                projectId=self.__project_id,
                datasetId=self.__dataset_id,
                tableId=self.__table_id).execute()

        # Remove schema cache
        self.__schema = None

    @property
    def schema(self):
        """Return schema dict.
        """

        # Create cache
        if getattr(self, '__schema', None) is None:

            # Get response
            response = self.__service.tables().get(
                    projectId=self.__project_id,
                    datasetId=self.__dataset_id,
                    tableId=self.__table_id).execute()

            # Get schema
            self.__schema = response['schema']

        return self.__schema

    def add_data(self, data):
        """Add data to table.

        Parameters
        ----------
        data: list
            List of data tuples.

        """

        # Convert data to byte stream csv
        bytes = io.BufferedRandom(io.BytesIO())
        class Stream: #noqa
            def write(self, string):
                bytes.write(string.encode('utf-8'))
        stream = Stream()
        writer = csv.writer(stream)
        for row in data:
            writer.writerow(row)
        bytes.seek(0)

        # Prepare job body
        body = {
            'configuration': {
                'load': {
                    'destinationTable': {
                        'projectId': self.__project_id,
                        'datasetId': self.__dataset_id,
                        'tableId': self.__table_id
                    },
                    'sourceFormat': 'CSV',
                }
            }
        }

        # Prepare job media body
        mimetype = 'application/octet-stream'
        media_body = MediaIoBaseUpload(bytes, mimetype=mimetype)

        # Make request to Big Query
        response = self.__service.jobs().insert(
            projectId=self.__project_id,
            body=body,
            media_body=media_body).execute()
        self.__wait_response(response)

    def get_data(self):
        """Return table's data.

        Returns
        -------
        generator
            Generator of data tuples.

        """

        # Get response
        response = self.__service.tabledata().list(
                projectId=self.__project_id,
                datasetId=self.__dataset_id,
                tableId=self.__table_id).execute()

        # Yield rows
        for row in response['rows']:
            yield tuple(field['v'] for field in row['f'])

    # Private

    def __wait_response(self, response):

        # Get job instance
        job = self.__service.jobs().get(
            projectId=response['jobReference']['projectId'],
            jobId=response['jobReference']['jobId'])

        # Wait done
        while True:
            result = job.execute(num_retries=1)
            if result['status']['state'] == 'DONE':
                if result['status'].get('errors'):
                    errors = result['status']['errors']
                    message = '\n'.join(error['message'] for error in errors)
                    raise RuntimeError(message)
                break
            time.sleep(1)
