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
SISKIN_ENV_ENABLED=1 python -m pytest -v cloud_optimized_dicom/tests

# Run specific test file
SISKIN_ENV_ENABLED=1 python -m pytest -v cloud_optimized_dicom/tests/test_cod_object.py

# Run specific test
python -m pytest -v cloud_optimized_dicom/tests/test_metadata_serialization.py::test_v2_round_trip
```

Note: `SISKIN_ENV_ENABLED=1` is required for tests that interact with GCP resources. Tests skip when this flag is absent.

### Code Formatting
Pre-commit hooks automatically run:
- `autoflake` - Remove unused imports
- `isort --profile=black` - Sort imports
- `black` - Format code

## Pull Requests & Releases

### PR title format
`<type>(PROC-XXXX): subject` — e.g. `feat(PROC-1502): backfill dropped UID tags`. Use `NO-ISSUE` in place of the Linear ID only when there is no associated ticket.

Allowed types and their effect on the next release:
- `feat` → minor bump (1.0.0 → 1.1.0)
- `fix` → patch bump (1.0.0 → 1.0.1)
- `feat!` (or `BREAKING CHANGE:` footer) → major bump (1.0.0 → 2.0.0)
- `chore`, `docs`, `refactor`, `test`, `ci`, `build`, `perf`, `revert` → no release

Squash-merge is the norm, so the PR title becomes the commit on `main` that release-please reads.

### Releases are automated
Do **not** edit the `version` field in `pyproject.toml` directly. [release-please](https://github.com/googleapis/release-please) runs from `.github/workflows/release.yml` on every push to `main`, continuously updating a "chore(main): release X.Y.Z" PR. Merging that Release PR:
1. Bumps the version in `pyproject.toml` and updates `CHANGELOG.md`
2. Tags `vX.Y.Z` and creates a GitHub release
3. The same workflow run then builds the wheel and publishes to PyPI via trusted publishing (no cross-workflow cascade, so `GITHUB_TOKEN` is sufficient — no PAT/App needed)

Config lives in `release-please-config.json` and `.release-please-manifest.json`. Manual re-publish fallback: `gh workflow run release.yml -f environment=pypi` (or `testpypi`).

## Architecture Overview

### Core Classes

**CODObject** (`cod_object.py`)
- Primary interface for interacting with cloud-optimized DICOM series
- Manages series-level tar archives and metadata in GCS
- Handles access modes and state synchronization
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
- `mode="e"`: Edit access; acquires exclusive lock automatically; requires the series to already exist (raises `CODObjectNotFoundError` if metadata or tar is missing); on context enter, fetches + extracts the tar so each `instance.dicom_uri` points at a local temp `.dcm` the caller can rewrite in place; on context exit, validates the instance UID set is unchanged, repacks the tar, rebuilds the sqlite index + series metadata, regenerates the thumbnail if pixel data changed, and uploads. Cannot add or remove instances — `append()` is blocked in this mode.
- `sync_on_exit=True` (default): Auto-syncs and releases lock on context exit for `mode="w"`, `mode="a"`, or `mode="e"`
- `sync_on_exit=False`: No lock acquired, no auto-sync; useful for local testing/debugging
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

**Pixel Data Compression** (`transcode.py`)
- `Instance.compress()` re-encodes pixel data as JPEG 2000 Lossless during `append()`
- Uncompressed sources are always re-encoded
- JPEG Lossless and RLE sources are re-encoded only when the result decodes back bit-exact and is smaller
- Lossy, JPEG 2000 and JPEG-LS sources keep their bytes
- `YBR_FULL` stays raw, RGB becomes `YBR_RCT`, and `YBR_FULL_422` passes through
- JPEG Lossless decodes with GDCM because pylibjpeg-libjpeg clamps instead of wrapping (gradient-beam PROC-1950)
- A codec failure keeps the original bytes and counts a `transcode_passthrough_*` metric

**Metadata Versions**
- v1.0: Uncompressed DICOM JSON dict, UIDs parsed from metadata
- v2.0: Zstandard-compressed metadata, explicit UID/pixeldata indexing, ~5-10x size reduction

### Project Structure

```
cloud_optimized_dicom/
├── cod_object.py          # Main CODObject class
├── instance.py            # Instance representation
├── instance_metadata.py   # Instance-level metadata handling
├── series_metadata.py     # Series-level metadata handling
├── append.py              # Instance appending logic
├── edit.py                # Edit-mode (mode="e") repack + validation
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
- `pydicom>=3.0`: Upstream pydicom 3
- `pylibjpeg-openjpeg`, `python-gdcm`: JPEG 2000 encoding and JPEG Lossless decoding for `Instance.compress()`
- `google-cloud-storage`: GCS operations
- `ratarmountcore`: Efficient tar file access
- `zstandard`: Metadata compression (v2.0)
- `smart-open`: Unified remote file access

**Optional:**
- `apache-beam[gcp]`: Data processing; install with `pip install cloud-optimized-dicom[beam]`. Without Beam, metric counters silently no-op.

**Test:**
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

# Edit access - edit mode (lock acquired, fetches + extracts existing tar, repacks on sync)
# Requires the series to already exist. Cannot add/remove instances.
with CODObject(client=..., datastore_path=..., mode="e") as cod:
    for instance in cod.get_instances().values():
        # instance.dicom_uri now points at a local .dcm the caller can rewrite
        ds = pydicom.dcmread(instance.dicom_uri)
        ds.PatientName = "REDACTED"
        ds.save_as(instance.dicom_uri)
# sync() called automatically: tar repacked, metadata rebuilt, thumbnail regenerated if pixel data changed

# Local testing (no lock, no sync - efficient for debugging)
with CODObject(client=..., datastore_path=..., mode="a", sync_on_exit=False) as cod:
    cod.append(instances)
# no lock acquired, no sync on exit

# Incorrect: Lock persists indefinitely
cod = CODObject(client=..., datastore_path=..., mode="w")
del cod  # Lock still exists remotely!
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
