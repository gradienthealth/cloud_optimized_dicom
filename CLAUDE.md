# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Setup
```bash
# Create virtual environment (Python 3.11 required; 3.14 not yet supported)
python3.11 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install with dev dependencies
pip install -e ".[dev]"

# Set up pre-commit hooks (required)
pre-commit install
```

### Testing
```bash
# Run all tests
SISKIN_ENV_ENABLED=1 python -m unittest discover -v cloud_optimized_dicom.tests

# Run specific test file
SISKIN_ENV_ENABLED=1 python -m unittest cloud_optimized_dicom.tests.test_cod_object -v

# Run specific test
python -m unittest cloud_optimized_dicom.tests.test_metadata_serialization.TestMetadataSerialization.test_v2_round_trip -v
```

Note: `SISKIN_ENV_ENABLED=1` is required for tests that interact with GCP resources. Tests skip when this flag is absent.

### Code Formatting
Pre-commit hooks automatically run:
- `autoflake` - Remove unused imports
- `isort --profile=black` - Sort imports
- `black` - Format code

## Architecture Overview

### Core Classes

**CODObject** (`cod_object.py`)
- Primary interface for interacting with cloud-optimized DICOM series
- Manages series-level tar archives and metadata in GCS
- Handles access modes, serialization/deserialization, and state synchronization
- Key URI pattern: `<datastore_path>/studies/<study_uid>/series/<series_uid>.tar`
- Must be used as context manager for `mode="w"` to ensure proper lock release and sync

**Instance** (`instance.py`)
- Represents a single DICOM file
- Tracks URI changes through ingestion lifecycle (remote → local → tar-nested)
- Manages dependencies (e.g., Intelerad `.dcm` + `.j2c` files)
- Supports UID hashing for de-identification
- Three URI fields: `dicom_uri` (current location), `_original_path` (immutable), `dependencies` (related files)

**SeriesMetadata** (`series_metadata.py`)
- Wraps the JSON metadata structure for a series
- Handles both v1.0 (uncompressed) and v2.0 (zstandard-compressed) metadata formats
- Provides instance management and metadata serialization

**DicomMetadata** (`instance_metadata.py`)
- Handles DICOM metadata at instance level
- v2.0: Metadata is zstandard-compressed and base64-encoded with explicit UID indexing
- v1.0: Metadata stored as raw JSON dict
- Lazy decompression with smart caching for small metadata (<1KB compressed)

### Key Concepts

**Access Modes**
- `mode="r"`: Read-only access; no lock acquired; allows all read operations
- `mode="w"`: Write access (overwrite); acquires exclusive lock automatically (raises `LockAcquisitionError` if exists); starts fresh with empty metadata/tar locally; overwrites remote tar/metadata on sync; never fetches remote tar
- `mode="a"`: Append access; acquires exclusive lock automatically (raises `LockAcquisitionError` if exists); fetches remote tar if it exists; appends to existing tar/metadata on sync
- `sync_on_exit=True` (default): Auto-syncs and releases lock on context exit for `mode="w"` or `mode="a"`
- `sync_on_exit=False`: No lock acquired, no auto-sync; useful for local testing/debugging
- Locks persist through serialization/deserialization (for Apache Beam workflows)
- Locks deliberately "hang" on errors to indicate series corruption
- User must use context manager for proper lock release

**Deprecated Parameters**
- `lock` parameter: Replaced by `mode`; emits DeprecationWarning if used
- `dirty` parameter on methods: No longer needed; emits DeprecationWarning if used
- `sync()` method: Called automatically on context exit; explicit calls emit DeprecationWarning

**UID Hashing**
- `Instance.uid_hash_func`: Optional callable for de-identification
- CODObject has no awareness of hashing; user must supply pre-hashed UIDs on instantiation
- Instance class provides both `study_uid()` and `hashed_study_uid()` methods
- Hashed UIDs used in metadata keys (`deid_study_uid` vs `study_uid`)

**Hints** (`hints.py`)
- Pre-known metadata (instance_uid, crc32c, size) to avoid unnecessary fetches
- Enables duplicate detection without downloading files
- Validated during ingestion to prevent datastore corruption

**Metadata Versions**
- v1.0: Uncompressed DICOM JSON dict, UIDs parsed from metadata
- v2.0: Zstandard-compressed metadata, explicit UID/pixeldata indexing, ~5-10x size reduction

**Serialization/Deserialization**
- CODObject designed for Apache Beam workflows
- `.serialize()` returns dict for passing between DoFns
- `.deserialize()` reconstructs object, can re-acquire persistent lock
- Pattern: Acquire lock in first DoFn, pass serialized object through pipeline, release in last DoFn

### Project Structure

```
cloud_optimized_dicom/
├── cod_object.py          # Main CODObject class
├── instance.py            # Instance representation
├── instance_metadata.py   # Instance-level metadata handling
├── series_metadata.py     # Series-level metadata handling
├── append.py              # Instance appending logic
├── locker.py              # CODLocker for lock management
├── hints.py               # Hints dataclass
├── errors.py              # Custom exception hierarchy
├── virtual_file.py        # VirtualFile for tar-nested access
├── custom_offset_tables.py # Multiframe offset table extraction
├── thumbnail.py           # Thumbnail generation and fetching
├── truncate.py            # Truncation and removal operations
├── dicomweb.py            # DICOMweb endpoint integration
├── metrics.py             # Metrics counters (Apache Beam compatible)
├── query_parsing.py       # Query parsing utilities
├── utils.py               # Shared utilities
└── tests/                 # Unit tests
```

### Dependencies

**Core:**
- `pydicom3`: Custom fork with namespace isolation (published package)
- `google-cloud-storage`: GCS operations
- `apache-beam[gcp]`: Data processing (CODObject serialization compatible)
- `ratarmountcore`: Efficient tar file access
- `zstandard`: Metadata compression (v2.0)
- `smart-open`: Unified remote file access

**Test:**
- `pydicom==2.3.0`: Original pydicom for validation
- `matplotlib`: Visualization in tests

### Important Patterns

**Instance URI Lifecycle:**
1. User creates: `Instance(dicom_uri="gs://bucket/file.dcm")`
2. `.open()` called: `dicom_uri` → temp local path
3. Appended to CODObject: `dicom_uri` → `local/series.tar://instances/{uid}.dcm`
4. Synced to datastore: Recorded remotely as `gs://datastore/series.tar://instances/{uid}.dcm`

**Context Manager Usage:**
```python
# Read-only access (no lock acquired)
with CODObject(client=..., datastore_path=..., mode="r") as cod:
    metadata = cod.get_metadata()
    instances = cod.get_instances()

# Write access - overwrite mode (lock acquired, starts fresh, overwrites on sync)
with CODObject(client=..., datastore_path=..., mode="w") as cod:
    cod.append(instances)
# sync() called automatically, lock released, overwrites remote tar/metadata

# Append access - append mode (lock acquired, fetches existing tar, appends on sync)
with CODObject(client=..., datastore_path=..., mode="a") as cod:
    cod.append(instances)
# sync() called automatically, lock released, appends to remote tar/metadata

# Local testing (no lock, no sync - efficient for debugging)
with CODObject(client=..., datastore_path=..., mode="a", sync_on_exit=False) as cod:
    cod.append(instances)
# no lock acquired, no sync on exit

# Incorrect: Lock persists indefinitely
cod = CODObject(client=..., datastore_path=..., mode="w")
del cod  # Lock still exists remotely!
```

**Apache Beam Pattern:**
```python
def first_dofn():
    cod = CODObject(..., mode="w")  # No context manager
    yield cod.serialize()  # Lock persists

def last_dofn(serialized_cod):
    with CODObject.deserialize(**serialized_cod) as cod:  # Reacquires lock
        pass  # Work happens here
    # sync() called automatically, lock released
```

### Testing Notes

- Tests require GCS authentication (service account JSON key in `GCP_SA_KEY` secret for CI)
- Test buckets: `siskin-172863-test-data`, `siskin-172863-temp`
- Test data directory: `cloud_optimized_dicom/tests/test_data/`
- Tests skip when `SISKIN_ENV_ENABLED` is not set
- GCP project: `gradient-pacs-siskin-172863`

### Error Handling

All custom errors inherit from `CODError`:
- `LockAcquisitionError`: Lock already exists
- `CODObjectNotFoundError`: Series not found when `create_if_missing=False`
- `WriteOperationInReadModeError`: Write operation attempted in read mode (`mode="r"`)
- `ErrorLogExistsError`: Error log exists in datastore (series corrupt)
- `TarValidationError`, `TarMissingInstanceError`, `HashMismatchError`: Integrity failures
- `HintMismatchError`: Hints don't match actual values
