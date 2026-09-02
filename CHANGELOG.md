# Changelog

## [2.1.0](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v2.0.3...v2.1.0) (2026-09-02)


### Features

* **PROC-2531:** re-encode legacy lossless JPEG and RLE to JPEG 2000 on ingest ([#150](https://github.com/gradienthealth/cloud_optimized_dicom/issues/150)) ([d3f1ed7](https://github.com/gradienthealth/cloud_optimized_dicom/commit/d3f1ed7f06c101c9a41a9b50416467139bb6fad7))

## [2.0.3](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v2.0.2...v2.0.3) (2026-06-03)


### Bug Fixes

* **PROC-1927:** respect compress flag in _handle_new ([#142](https://github.com/gradienthealth/cloud_optimized_dicom/issues/142)) ([e1d2a3b](https://github.com/gradienthealth/cloud_optimized_dicom/commit/e1d2a3b1ab308aa753b2158c155455c96c76c1ea))

## [2.0.2](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v2.0.1...v2.0.2) (2026-05-13)


### Bug Fixes

* **PROC-1688:** silence ratarmountcore print noise in logs ([#139](https://github.com/gradienthealth/cloud_optimized_dicom/issues/139)) ([1a9cff2](https://github.com/gradienthealth/cloud_optimized_dicom/commit/1a9cff261e4acae271b4983a82ed49f802ca662d))

## [2.0.1](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v2.0.0...v2.0.1) (2026-05-07)


### Bug Fixes

* **PROC-1669:** relax google-cloud-storage and apache-beam pins ([#137](https://github.com/gradienthealth/cloud_optimized_dicom/issues/137)) ([344772e](https://github.com/gradienthealth/cloud_optimized_dicom/commit/344772e878afe9da0e8ca04f7761d120414f87fa))

## [2.0.0](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v1.2.0...v2.0.0) (2026-05-07)


### ⚠ BREAKING CHANGES

* **PROC-1668:** `cloud_optimized_dicom` now depends on upstream `pydicom>=3.0` instead of the `pydicom3` fork. Downstream impact:
    - `isinstance` checks against `pydicom3.*` on cod-returned objects will start returning `False` — switch to `pydicom.*`. (This is exactly the bug PROC-1668 fixes for gradient-beam.)
    - Installing `cloud-optimized-dicom` no longer transitively installs `pydicom3`. Code that imported `pydicom3` only because cod brought it in must either switch to `import pydicom` or add the fork to its own dependencies.
    - Environments pinned to `pydicom<3` will no longer resolve. Bump to `pydicom>=3.0`.


### Features

* **PROC-1668:** drop pydicom3 fork in favor of upstream pydicom ([#134](https://github.com/gradienthealth/cloud_optimized_dicom/pull/134)) ([42d5e7e](https://github.com/gradienthealth/cloud_optimized_dicom/commit/42d5e7eb8110c120a7feb41085a5efc3270ef586))

## [1.2.0](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v1.1.1...v1.2.0) (2026-04-30)


### Features

* **PROC-1576:** add BoundingBox and PixelRedaction dataclasses ([#126](https://github.com/gradienthealth/cloud_optimized_dicom/issues/126)) ([45d2a70](https://github.com/gradienthealth/cloud_optimized_dicom/commit/45d2a705400bdd3d770250a7b95768e3a5fe4d79))
* **PROC-1576:** redact pixel data via bounding boxes in edit mode ([#125](https://github.com/gradienthealth/cloud_optimized_dicom/issues/125)) ([097981e](https://github.com/gradienthealth/cloud_optimized_dicom/commit/097981ea4a5f912fbeb472158177d62c448d7428))

## [1.1.1](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v1.1.0...v1.1.1) (2026-04-29)


### Bug Fixes

* **NO-ISSUE:** migrate to ratarmountcore 0.10 mountsource API ([#123](https://github.com/gradienthealth/cloud_optimized_dicom/issues/123)) ([18a4224](https://github.com/gradienthealth/cloud_optimized_dicom/commit/18a422481e869e850a4876a49ba13e36511960e9))
* **PROC-940:** unpin urllib3 now that google-resumable-media ships the fix ([#120](https://github.com/gradienthealth/cloud_optimized_dicom/issues/120)) ([3ba10c6](https://github.com/gradienthealth/cloud_optimized_dicom/commit/3ba10c6f7f05aee5ef152f33d57f91c6bc73f878))


### Documentation

* **NO-ISSUE:** document scoped SA permissions and dependabot secret ([#122](https://github.com/gradienthealth/cloud_optimized_dicom/issues/122)) ([6ca4721](https://github.com/gradienthealth/cloud_optimized_dicom/commit/6ca47215fa1201fee0f46dbe0c3f581d9d6bf4b5))

## [1.1.0](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v1.0.0...v1.1.0) (2026-04-29)


### Features

* **PROC-1548:** add edit mode for in-place DICOM modification ([#102](https://github.com/gradienthealth/cloud_optimized_dicom/issues/102)) ([4375317](https://github.com/gradienthealth/cloud_optimized_dicom/commit/4375317c69d62eb2cd929236072257944b38e9b3))

## [1.0.0](https://github.com/gradienthealth/cloud_optimized_dicom/compare/v0.2.5...v1.0.0) (2026-04-21)


### Miscellaneous Chores

* **PROC-1514:** release v1.0.0 and remove release-as pin ([#99](https://github.com/gradienthealth/cloud_optimized_dicom/issues/99)) ([c5b1b5b](https://github.com/gradienthealth/cloud_optimized_dicom/commit/c5b1b5b33af7b94a9fb0bd4bbbfdfe73f992de62))
