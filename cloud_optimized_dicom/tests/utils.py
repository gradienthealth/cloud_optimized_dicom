from google.cloud import storage


def delete_uploaded_blobs(client: storage.Client, uris_to_delete: list[str]):
    """
    Helper method used by tests to delete blobs they have created, resetting the test
    environment for a subsequent test. Takes a GCS client and a list of GCS uris to delete.
    These URIs should be folders (e.g. 'gs://siskin-172863-test-data/concat-output'), and
    this method will delete everything in the folder
    """
    for gcs_uri in uris_to_delete:
        bucket_name, folder_name = gcs_uri.replace("gs://", "").split("/", 1)
        for blob in client.list_blobs(bucket_name, prefix=f"{folder_name}/"):
            blob.delete()
