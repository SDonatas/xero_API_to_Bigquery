import pathlib
from google.cloud import storage
import logging
import json
import os


#environmental path
path = str(pathlib.Path(__file__).parent)

#Logging
logger = logging.getLogger(__name__)

#Settings Json
settingsJsonBucket = json.load(open(path + "/../conf/gcloud_bucket.json", 'r'))
serviceAccount = json.load(open(path + "/../conf/bigquery_settings.json", 'r'))


class gcloud_bucket:
    def __init__(self, settingsJsonBucket = settingsJsonBucket, serviceAccount=serviceAccount):
        self.settings = settingsJsonBucket
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path + "/.." + serviceAccount['GOOGLE_APPLICATION_CREDENTIALS']
        

    def upload_blob_from_file(self, source_file_name):
        """Uploads a file to the bucket."""
        # bucket_name = "your-bucket-name"
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(self.settings['BUCKET_NAME'])
        blob = bucket.blob(self.settings['BLOB_NAME'])

        blob.upload_from_filename(source_file_name)

        logger.info(
            "File {} uploaded to {}.".format(
                source_file_name, self.settings['BLOB_NAME']
            )
        )

    def upload_blob_from_string(self, payload: str):
        """Uploads a file to the bucket."""
        # bucket_name = "your-bucket-name"
        # source_file_name = "local/path/to/file"
        # destination_blob_name = "storage-object-name"

        storage_client = storage.Client()
        bucket = storage_client.bucket(self.settings['BUCKET_NAME'])
        blob = bucket.blob(self.settings['BLOB_NAME'])
        blob.upload_from_string(payload)

        logger.info("String uploaded to {}.".format(self.settings['BLOB_NAME']))

    def download_blob_to_file(self, destination_file_name: str):
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"

        storage_client = storage.Client()

        bucket = storage_client.bucket(self.settings['BUCKET_NAME'])
        blob = bucket.blob(self.settings['BLOB_NAME'])
        blob.download_to_filename(destination_file_name)

        logger.info(
            "Blob {} downloaded to {}.".format(
                self.settings['BLOB_NAME'], destination_file_name
            )
        )


    def download_blob_to_string(self) -> str:
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"

        storage_client = storage.Client()

        bucket = storage_client.bucket(self.settings['BUCKET_NAME'])
        blob = bucket.blob(self.settings['BLOB_NAME'])
        payload = blob.download_as_string()

        logger.info(
            "Blob {} downloaded as string.".format(
                self.settings['BLOB_NAME']
            )
        )

        return payload


    def delete_blob(self):
        """Deletes a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # blob_name = "your-object-name"

        storage_client = storage.Client()

        bucket = storage_client.bucket(self.settings['BUCKET_NAME'])
        blob = bucket.blob(self.settings['BLOB_NAME'])
        blob.delete()

        logger.info("Blob {} deleted.".format(self.settings['BLOB_NAME']))
