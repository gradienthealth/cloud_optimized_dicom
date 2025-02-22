# Cloud Optimized DICOM

A library for efficiently storing and interacting with DICOM files in the cloud.

## Concepts

### dicom_uri
Where the actual DICOM file lives. Requried argument for instantiation of an instance.

### dependencies
(OPTIONAL) Files that were required to generate `dicom_uri`.
Intelerad instances, for example, may have `.dcm` and `.j2c` files that needed to be combined in order to create the true dicom P10 file.

