STUDY_UID = "1.2.826.0.1.3680043.8.498.85986080985748066693633351924707464088"
SERIES_UID = "1.2.826.0.1.3680043.8.498.53950754827956461313105393044601247551"
INSTANCE_UIDS = [
    "1.2.826.0.1.3680043.8.498.10838996796699162364447867308902157301",
    "1.2.826.0.1.3680043.8.498.13615419375856807118067401311662947193",
    "1.2.826.0.1.3680043.8.498.14232321135552375992380719347498317286",
]
BUCKET_NAME = "siskin-172863-test-data"
GOLDEN_URI_PREFIX = "gs://siskin-172863-test-data/golden"
PLAYGROUND_URI_PREFIX = "gs://siskin-172863-test-data/playground"
OUTPUT_URI = "gs://siskin-172863-test-data/concat-output"
FILE1 = {
    "file_uri": f"{PLAYGROUND_URI_PREFIX}/{STUDY_UID}/series/{SERIES_UID}/instances/{INSTANCE_UIDS[0]}.dcm",
    "size": 258118,
    "crc32c": "1VFoRg==",
    "instance_uid": INSTANCE_UIDS[0],
}
FILE1_NEW_VERSION = {
    "file_uri": f"{PLAYGROUND_URI_PREFIX}/{STUDY_UID}/series/{SERIES_UID}/instances/{INSTANCE_UIDS[0]}_v2.dcm",
    "size": 258118,
    "crc32c": "1VFoRg==",
    "instance_uid": INSTANCE_UIDS[0],
}
FILE2 = {
    "file_uri": f"{PLAYGROUND_URI_PREFIX}/{STUDY_UID}/series/{SERIES_UID}/instances/{INSTANCE_UIDS[1]}.dcm",
    "size": 270186,
    "crc32c": "21UzbQ==",
    "instance_uid": INSTANCE_UIDS[1],
}
FILE3 = {
    "file_uri": f"{PLAYGROUND_URI_PREFIX}/{STUDY_UID}/series/{SERIES_UID}/instances/{INSTANCE_UIDS[2]}.dcm",
    "size": 270058,
    "crc32c": "t+Jnkw==",
    "instance_uid": INSTANCE_UIDS[2],
}
GROUPING_FULL = {
    "study_uid": STUDY_UID,
    "series_uid": SERIES_UID,
    "files": [FILE1, FILE2, FILE3],
}
GROUPING_SINGLE = {"study_uid": STUDY_UID, "series_uid": SERIES_UID, "files": [FILE1]}
GROUPING_FIRST_TWO = {
    "study_uid": STUDY_UID,
    "series_uid": SERIES_UID,
    "files": [FILE1, FILE2],
}
GROUPING_LAST_TWO = {
    "study_uid": STUDY_UID,
    "series_uid": SERIES_UID,
    "files": [FILE2, FILE3],
}
GROUPING_INCLUDING_DUPE = {
    "study_uid": STUDY_UID,
    "series_uid": SERIES_UID,
    "files": [FILE1, FILE1_NEW_VERSION],
}
