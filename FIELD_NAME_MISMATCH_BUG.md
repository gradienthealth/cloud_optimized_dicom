# Critical Bug: CRC32C Field Name Mismatch

## The Bug

**BigQuery query** (`gradient-beam/components/cloud_optimized_dicom/gcs/io.py:62`):
```sql
file.crc32c AS crc32c_hash
```

**Hint parser** (`cloud_optimized_dicom/query_parsing.py:50`):
```python
crc32c=file.get("crc32c"),  # Looking for "crc32c"
```

**Result**: `crc32c=None` for every file, even though BigQuery provides it!

## Impact

This bug causes:
- **76% of CPU wasted** on redundant CRC32C computation
- **~30 vCPU** consumed unnecessarily
- **Reading 900+ GB** of data from GCS for hashing
- **2+ hours** of extra processing time per job

## The Fix

### Option 1: Fix BigQuery Query (Recommended)

Change `gradient-beam/components/cloud_optimized_dicom/gcs/io.py:62`:

```sql
# Before:
file.crc32c AS crc32c_hash

# After:
file.crc32c AS crc32c
```

### Option 2: Fix Hint Parser

Change `cloud_optimized_dicom/query_parsing.py:50`:

```python
# Before:
crc32c=file.get("crc32c"),

# After:
crc32c=file.get("crc32c_hash"),
```

**Recommendation**: Fix the BigQuery query (Option 1) to match the expected schema.

## Expected Results After Fix

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Total vCPU | 42 | ~11 | 74% reduction |
| CRC32C computations | 902,707 (100%) | ~9,027 (1%) | 99% reduction |
| Data read for hashing | ~900 GB | ~9 GB | 99% reduction |
| Cost per job | High | Low | 74% reduction |

## Verification

After deploying the fix, check:

1. **Logs should show hints being used**:
   ```python
   # Add logging in instance.py:validate()
   if self.hints.crc32c is not None:
       logger.info(f"Using CRC32C hint: {self.hints.crc32c}")
   else:
       logger.info(f"Computing CRC32C (no hint)")
   ```

2. **Metrics to track**:
   - Add counter for "crc32c_hint_used"
   - Add counter for "crc32c_computed"
   - Expect 99% hint usage for duplicate files

3. **Job metrics**:
   - vCPU should drop from 42 to ~11
   - Job duration should decrease significantly

## Root Cause

This mismatch was introduced when the BigQuery schema used `crc32c_hash` but the hint parser expected `crc32c`. The field name standardization was incomplete.

## Related Code

- **BigQuery query generator**: `/Users/sai/Documents/gradienthealth/gradient-beam/components/cloud_optimized_dicom/gcs/io.py:36-71`
- **Hint parser**: `/Users/sai/Documents/gradienthealth/cloud_optimized_dicom/cloud_optimized_dicom/query_parsing.py:33-64`
- **Instance validation**: `/Users/sai/Documents/gradienthealth/cloud_optimized_dicom/cloud_optimized_dicom/instance.py:120-155`
- **CRC32C computation**: `/Users/sai/Documents/gradienthealth/cloud_optimized_dicom/cloud_optimized_dicom/utils.py:124-136`

## Timeline

1. ⬜ Fix field name mismatch (1 line change)
2. ⬜ Add metrics to track hint usage
3. ⬜ Test with small job
4. ⬜ Deploy to production
5. ⬜ Verify 74% CPU reduction
