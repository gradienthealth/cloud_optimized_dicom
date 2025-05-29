import os

from google.cloud import storage

from cloud_optimized_dicom.cod_object import CODObject


# Reference: https://www.dicomstandard.org/docs/librariesprovider2/dicomdocuments/dicom/wp-content/uploads/2018/04/dicomweb-cheatsheet.pdf
def parse_dicomweb_request(request: str):
    """
    Handle a dicomweb request of format "GET {s}/studies/{study}/series/{series}"
    """
    assert request.startswith("GET "), "Only GET requests are currently supported"
    uri = request.replace("GET", "").strip()
    assert uri.startswith("gs://"), "Only gs:// URIs are supported"
    assert "?" not in uri, "Query parameters are not supported"
    assert "/studies/" in uri, "Invalid request format"
    subpath_index = uri.index("studies/")
    datastore_uri = uri[:subpath_index].rstrip("/")
    subpath = uri[subpath_index:]
    subpath_parts = subpath.split("/")
    assert (
        len(subpath_parts) % 2 == 0
    ), f"Expected even number of parts in subpath: {subpath}"
    # map odd indices to subsequet even ones in dict
    request_dict = {"datastore_uri": datastore_uri}
    for i in range(0, len(subpath_parts), 2):
        request_dict[subpath_parts[i]] = subpath_parts[i + 1]
    return request_dict


def handle_dicomweb_request(request: str, client: storage.Client):
    """
    Handle a dicomweb request of format "GET {s}/studies/{study}/series/{series}"
    """
    request_dict = parse_dicomweb_request(request)
    if "frames" in request_dict:
        return _handle_frame_level_request(request_dict, client)
    if "instances" in request_dict:
        return _handle_instance_level_request(
            datastore_uri=request_dict["datastore_uri"],
            study_uid=request_dict["studies"],
            series_uid=request_dict["series"],
            instance_uid=request_dict["instances"],
            client=client,
        )
    if "series" in request_dict:
        return _handle_series_level_request(
            datastore_uri=request_dict["datastore_uri"],
            study_uid=request_dict["studies"],
            series_uid=request_dict["series"],
            client=client,
        )
    if "studies" in request_dict:
        return _handle_study_level_request(
            datastore_uri=request_dict["datastore_uri"],
            study_uid=request_dict["studies"],
            client=client,
        )
    raise ValueError("Invalid request format")


def _handle_frame_level_request(request_dict: dict, client: storage.Client):
    raise NotImplementedError("frame level requests not yet supported")


def _handle_instance_level_request(
    datastore_uri: str,
    study_uid: str,
    series_uid: str,
    instance_uid: str,
    client: storage.Client,
):
    series_dict = _handle_series_level_request(
        datastore_uri, study_uid, series_uid, client
    )
    return series_dict["instances"][instance_uid]["metadata"]


def _handle_series_level_request(
    datastore_uri: str, study_uid: str, series_uid: str, client: storage.Client
):
    cod_obj = CODObject(
        datastore_path=datastore_uri,
        client=client,
        study_uid=study_uid,
        series_uid=series_uid,
        lock=False,
        create_if_missing=False,
    )
    return cod_obj.get_metadata(dirty=True).to_dict()["cod"]


def _handle_study_level_request(
    datastore_uri: str, study_uid: str, client: storage.Client
):
    # Parse the GCS URI into bucket and prefix
    study_uri = os.path.join(datastore_uri, "studies", study_uid)
    # Remove gs:// prefix and split into bucket and prefix
    path_without_prefix = study_uri.replace("gs://", "")
    bucket_name = path_without_prefix.split("/")[0]
    prefix = "/".join(path_without_prefix.split("/")[1:])

    # List blobs in the study directory
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    series_uids = [
        blob.name.split("/")[-1].rstrip(".tar")
        for blob in blobs
        if blob.name.endswith(".tar")
    ]
    study_dict = {}
    for series_uid in series_uids:
        study_dict[series_uid] = _handle_series_level_request(
            datastore_uri, study_uid, series_uid, client
        )
    return study_dict
