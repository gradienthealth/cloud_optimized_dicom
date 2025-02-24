# Cloud Optimized DICOM

A library for efficiently storing and interacting with DICOM files in the cloud.

## Instance concepts

### `dicom_uri`
Where the actual DICOM file lives. Requried argument for instantiation of an `Instance`.

### `dependencies`
(OPTIONAL) File URIs that were required to generate `dicom_uri`.

After ingestion, one can conveniently delete these files by calling `Instance.delete_dependencies()`.

In most cases, `dicom_uri` will be the only dependency - the DICOM file is self-contained.

There are more complex cases to consider as well. Intelerad data, for example, may have `.dcm` and `.j2c` files that needed to be combined in order to create the true dicom P10 file.
In this case, `dicom_uri` is not meaningful in the context of deletion (it's likely a temp path on disk), and `dependencies` would be the `.dcm` and `.j2c` files.

### `Hints`
Metadata about the DICOM file that can be used to validate the file.

Say for example you have run some sort of inventory report on a set of DICOM files, and you now know their instance_uid and crc32c hash.

When ingesting these files using COD, you can provide this information via the Hints argument.

COD can then use the instance_uid and crc32c hash to determine whether this new instance is a duplicate without ever having to actually fetch the file.

Information provided in Hints is validated when the instance is fetched (i.e. during ingestion if the instance is NOT a duplicate), 
so that if user-provided hints are incorrect the COD datastore is not corupted.

## COD Object Concepts

TBD