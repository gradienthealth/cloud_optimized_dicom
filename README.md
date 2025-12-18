# Cloud Optimized DICOM

A library for efficiently storing and interacting with DICOM files in the cloud.

# Development Setup

## Prerequisites

- Python 3.11 or higher (Note: Python 3.14 is not yet supported due to build system compatibility issues)
- pip

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd cloud_optimized_dicom
```

2. Create and activate a virtual environment:
```bash
python3.11 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install the package in editable mode:
```bash
pip install -e .
```

4. To install with development dependencies (includes pre-commit and test dependencies):
```bash
pip install -e ".[dev]"
```

5. Set up pre-commit hooks (required for development):
```bash
pre-commit install
```

Alternatively, to install only test dependencies without pre-commit:
```bash
pip install -e ".[test]"
```

## Running Tests

```bash
SISKIN_ENV_ENABLED=1 python -m unittest discover -v cloud_optimized_dicom.tests
```

## Project Structure

The project uses `pyproject.toml` for package configuration and dependency management. Key dependencies include:
- `pydicom3`: Custom fork of pydicom with namespace isolation
- `google-cloud-storage`: For cloud storage operations
- `apache-beam[gcp]`: For data processing pipelines
- `zstandard`: For metadata compression (v2.0)

# Concepts & Design Philosophy

## Hashed vs. regular study/series/instance UIDs
Depending on your use case, you may notice that instances have 2 getter methods for each UID: 
1. standard: `{study/series/instance}_uid()`
2. hashed: `hashed_{study/series/instance}_uid()`.

If your use case is purely storage related (say you're a hospital using COD to store your data), you can just use the standard getters and not worry about hashing functionality at all.

If, however, your use case is de-identification related, you will likely be interested in COD's hashing functionality (outlined below).

### `CODObject` UIDs are used directly
For simplicity, only the `Instance` class deals with hashing. 
The `CODObject` class itself has no notion of hashed versus standard UIDs. 
The study/series UIDs provided to a `CODObject` on instantiation are the ones it uses directly, no querstions asked.

So, **if CODObject study/series UIDs are supposed to be hashed or otherwise modified, it is the responsibility of the user to supply the modified UIDs on instantiation**

### `Instance.uid_hash_func`
The Instance class has an argument called `uid_hash_func: Callable[[str], str] = None`.

This is expected to be a user-provided hash function that takes a string (the raw uid) and returns a string (the hashed uid).

By default (if unspecified), this function is `None`.

The existence of `uid_hash_func` (or lack thereof) is used in various key scenarios to decide whether hashed or standard UIDs will be used, including:
- determining whether an instance "belongs" to a cod object (has same study/series UIDs)
- choosing keys for UID related data in CODObject metadata dict (`deid_study_uid` vs. `study_uid`)

As a safety feature, if `instance.hashed_{study/series/instance}_uid()` is called but `instance.uid_hash_func` was not provided, a `ValueError` is raised.

## "Locking" as a race-case solution
### Motivation
Say there are multiple processes interacting with a COD datastore simultaneously.
These could be entirely separate processes, or one job with multiple workers.

In either case, what happens if they both attempt to modify the same `CODObject` at the same time?

To avoid the "first process gets overwritten by second process" outcome, we introduce the concept of "locking".

### Terminology & Concepts
A **lock** is just a file with a specific name (by default, `.cod.lock`).

**Acquiring a lock** means that the `CODObject` will upload a lock blob to the datastore and store its generation number. If the lock already exists, the `CODObject` will raise a `LockAcquisitionError`.

**State change operations** are any operations that constitute a change to the datastore (namely, appending to it).

By default, state change operations are **clean**, but they can also be **dirty**, meaning they are confined to the user's local environment and will not alter the remote datastore

### The `CODObject(lock=?)` argument
`CODObject`s take a `lock` argument which defaults to `None`. Instantiation behavior depends on this flag:

- `lock=None` -> error is raised (user is required to acknowledge their lock choice by setting this flag).
- `lock=True` -> `CODObject` will attempt to acquire a lock, and will raise an error if it cannot.
- `lock=False` -> `CODObject` will not attempt to acquire a lock. Any regular state change operations that are attempted will raise an error. dirty state change operations will be permitted, but the user will again be required to acknowledge the dirtiness of the operation by setting dirty=True in the operation call. See the state change operations section below for more info.

Because `CODObject(lock=True)` instantiation raises an error if the lock cannot be acquired (already exists), it is guaranteed that no other writing-enabled `CODObject(lock=True)` will be created on the same series while one already exists, thus avoiding the race condition where two workers attempt to create CODObjects with the same study/series UIDs.

### When is a lock necessary?
When the operation you are attempting involves actually modifying the COD datastore itself (example: ingesting new files), a lock is required

### Why would I ever set `lock=False`?
In some cases, like exporting or otherwise just reading data from COD but not altering it, you may not want your operation to be blocked if another process is interacting with the datastore.

### Lock Release & Management
`CODObject` is designed to be used as a context manager.
When you enter a `with` statement using a `CODObject(lock=True)`, the lock will persist for the duration of the statement, and will be released when the statement ends.
This way, all cleanup (including lock release) is handled for you.
```python
with CODObject(client=..., datastore_path=..., lock=True) as cod:
    cod.append(instances)
    cod.sync()
    # lock exists within context
    assert cod._locker.get_lock_blob().exists() is True
# lock is released when context is exited
assert cod._locker.get_lock_blob().exists() is False
```
In the case of an error, locks are deliberately left **hanging** to indicate that the series is corrupt in some way and needs user attention.

```python
with CODObject(client=..., datastore_path=..., lock=True) as cod:
    raise ValueError("test")
# assertion will pass; lock file persists
assert cod._locker.get_lock_blob().exists() is True
```

Locks are NOT automatically released when a `CODObject` goes out of scope,
which is an explicit design choice to allow for lock persistence across serialization/deserialization (see below).

The tradeoff, however, is that it is possible to accidentally create hanging locks:
```python
cod_a = CODObject(client=..., datastore_path=..., lock=True)
# do some stuff
cod_a.append(instances)
cod_a.sync()
del cod_a
# cod_a is now out of scope, but lock still exists in the remote datastore
cod_b = CODObject(client=..., datastore_path=..., lock=True)
# the above will raise a LockAcquisitionError because the lock persists
```
**It is YOUR responsibility as the user of this class to make sure your locks are released.**

## Serialization/Deserialization
COD was designed with apache beam workflows in mind. For this reason, `CODObject`s can be serialized into a dictionary, so that they can be conveniently shuffled or otherwise passed between `DoFn`s. 

Furthermore, because CODObjects store lock generation numbers, they can actually re-acquire an existing lock if they had it previously and were serialized/deserialized. Consider the following recommended workflow:
```python
def dofn_first():
    # note the LACK of "with" context manager here
    cod_obj = CODObject(client=..., datastore_path=..., lock=True)
    # do some stuff
    yield cod_obj.serialize() # lock persists

# ... (other dofns here, also without context managers)

def dofn_last(serialized_cod):
    # persistent lock reacquired during deserialization
    with CODObject.deserialize(**serialized_cod, client=...) as cod_obj:
        # do some stuff
        cod_obj.append(instances)
        cod_obj.sync()
    # lock released when "with" block exited
```

It would of course work perfectly well to use a `with` statement in each `DoFn`, 
but it would be unnecessarily inefficient as a unique lock would be acquired and released in each `DoFn`.


## Instance URI management: `dicom_uri` vs `_original_path` vs `dependencies`
Two main principles govern how the `Instance` class manages URIs:
1. It should be as simple and straightforward as possible to instantiate an `Instance`
2. There should be a single source of truth for where dicom data is actually located at all times

In keeping with these, there are three different class variables designed to manage URIs:
- `dicom_uri`: where the actual dcm data is located at any given moment. This is the only argument required to instantiate an `Instance`, 
and may change from what the user provided in order to accurately reflect the location of the dicom data (see example below)
- `_original_path`: private field automatically set to the same value as `dicom_uri` during `Instance` initialization.
- `dependencies`: (OPTIONAL) a user-defined list of URI strings that are related to this `Instance`, which theoretically could be deleted safely if the instance was synced to a COD Datastore

Because the actual location of dicom data changes throughout the ingestion process, `dicom_uri` changes to reflect this. Consider the following example:
1. User creates `instance = Instance(dicom_uri="gs://some-bucket/example.dcm")`.
At this point, `dicom_uri=_original_path="gs://some-bucket/example.dcm"`
2. User calls `instance.open()` to view the data. This causes the file to be fetched from its remote URI, and at this point `dicom_uri=path/to/a/local/temp/file/that/got/generated`. 
However, `_original_path` will never change and still points to  `gs://some-bucket/example.dcm`
3. User appends `instance` to a `CODObject`. After a successful append the instance will be located in the `CODObject`'s series-level tar on disk, so `dicom_uri=local/path/to/cod/series.tar://instances/{instance_uid}.dcm`.
4. User `sync`s the `CODObject` to the datastore. Because the instance still exists on disk in the local series tar, `instance.dicom_uri` does not change. However, in the remote COD datastore, the instance is recorded as having `dicom_uri="gs://cod/datastore/series.tar://instances/{instance_uid}.dcm"`

## `Hints`
Metadata about the DICOM file that can be used to validate the file.

Say for example you have run some sort of inventory report on a set of DICOM files, and you now know their `instance_uid` and `crc32c` hash.

When ingesting these files using COD, you can provide this information via the `Hints` argument.

COD can then use the `instance_uid` and hash to determine whether this new instance is a duplicate without ever having to actually fetch the file, 
thus avoiding unncessary costs associated with "no-op" ingestions (if ingestion job were to be mistakenly run twice, for example).

To avoid corrupting the COD datastore in the case of incorrect `Hint` values, 
information provided in `Hints` is validated when the instance is fetched (i.e. during ingestion if the instance is NOT a duplicate), 
so that if user-provided hints are incorrect the COD datastore is not corupted.

## The need for `Instance.dependencies`
In most cases, `dicom_uri` will be the only dependency - the DICOM file is self-contained.

However, there are more complex cases to consider. Intelerad data, for example, may have `.dcm` and `.j2c` files that needed to be combined in order to create the true dicom P10 file.
In this case, `dicom_uri` is not meaningful in the context of deletion (it's likely a temp path on disk), and `dependencies` would be the `.dcm` and `.j2c` files.

After ingestion, one can conveniently delete these files by calling `Instance.delete_dependencies()`.

# Metadata format

COD supports two metadata formats: v1.0 (legacy) and v2.0 (current). The formats differ primarily in how DICOM metadata is stored and whether certain fields are explicitly indexed.

## Metadata v2.0 (Current)

Version 2.0 introduces several optimizations:
- **Compressed metadata**: DICOM metadata is zstandard-compressed and base64-encoded to reduce storage size (typically achieves 5-10x compression on JSON)
- **Explicit UID indexing**: Study, Series, and Instance UIDs are stored as top-level fields for faster querying without decompression
- **Explicit pixeldata flag**: `has_pixeldata` boolean stored at top level
- **Lazy decompression**: Metadata is only decompressed when accessed via `instance.metadata`
- **Smart caching**: Small metadata (compressed size < 1KB) is cached after first decompression

Instance metadata structure (within `cod.instances`):
```json
{
  "instance_uid": "1.2.3.4.5",
  "series_uid": "1.2.3.4",
  "study_uid": "1.2.3",
  "has_pixeldata": true,
  "metadata": "<base64-encoded zstandard-compressed DICOM JSON dict>",
  "uri": "gs://.../series.tar://instances/{instance_uid}.dcm",
  "headers": {"start_byte": 123, "end_byte": 456},
  "offset_tables": {"CustomOffsetTable": [...], "CustomOffsetTableLengths": [...]},
  "crc32c": "the_blob_hash",
  "size": 123,
  "original_path": "path/where/this/file/was/originally/located",
  "dependencies": ["path/to/a/dependency", ...],
  "diff_hash_dupe_paths": ["path/to/a/duplicate", ...],
  "version": "2.0",
  "modified_datetime": "2024-01-01T00:00:00"
}
```

## Metadata v1.0 (Legacy)

Version 1.0 stores metadata uncompressed:
- **Uncompressed metadata**: Full DICOM JSON dict stored inline
- **UIDs parsed from metadata**: UIDs must be extracted from the metadata dict when needed
- **Pixeldata detection**: Presence of tag `7FE00010` in metadata indicates pixeldata

Instance metadata structure (within `cod.instances`):
```json
{
  "metadata": {
    "00080018": {"vr": "UI", "Value": ["1.2.3.4.5"]},
    "0020000D": {"vr": "UI", "Value": ["1.2.3"]},
    "0020000E": {"vr": "UI", "Value": ["1.2.3.4"]},
    ...
  },
  "uri": "gs://.../series.tar://instances/{instance_uid}.dcm",
  "headers": {"start_byte": 123, "end_byte": 456},
  "offset_tables": {"CustomOffsetTable": [...], "CustomOffsetTableLengths": [...]},
  "crc32c": "the_blob_hash",
  "size": 123,
  "original_path": "path/where/this/file/was/originally/located",
  "dependencies": ["path/to/a/dependency", ...],
  "diff_hash_dupe_paths": ["path/to/a/duplicate", ...],
  "version": "1.0",
  "modified_datetime": "2024-01-01T00:00:00"
}
```

## Complete COD Object Structure

Both versions use the same overall structure:
```json
{
  "deid_study_uid": "deid(StudyInstanceUID)",
  "deid_series_uid": "deid(SeriesInstanceUID)",
  "cod": {
    "instances": {
      "deid(SOPInstanceUID)": { /* instance metadata (v1 or v2 format) */ }
    }
  },
  "thumbnail": {
    "version": "1.0",
    "uri": "studies/{deid(StudyInstanceUID)}/series/{deid(SeriesInstanceUID)}.(mp4|jpg)",
    "thumbnail_index_to_instance_frame": [["deid(SOPInstanceUID)", frame_index], ...],
    "instances": {
      "deid(SOPInstanceUID)": {
        "frames": [
          {
            "thumbnail_index": 0,
            "anchors": {
              "original_size": {"width": 100, "height": 200},
              "thumbnail_upper_left": {"row": 0, "col": 10},
              "thumbnail_bottom_right": {"row": 127, "col": 117}
            }
          }
        ]
      }
    }
  },
  "other": {}
}
```