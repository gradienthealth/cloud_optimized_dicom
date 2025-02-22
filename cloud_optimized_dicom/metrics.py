from apache_beam.metrics import Metrics

NAMESPACE = "cloud_optimized_dicom"

# deletion metrics
NUM_DELETES = Metrics.counter(NAMESPACE, "num_deletes")
BYTES_DELETED_COUNTER = Metrics.counter(NAMESPACE, "bytes_deleted")
DEP_DOES_NOT_EXIST = Metrics.counter(NAMESPACE, "dep_does_not_exist")
INSTANCE_BLOB_CRC32C_MISMATCH = Metrics.counter(
    NAMESPACE, "instance_blob_crc32c_mismatch"
)
