# GCP Setup for GitHub Actions

This guide explains how to set up Google Cloud Platform authentication for running tests in GitHub Actions.

## Prerequisites

- Access to the GCP project `gradient-pacs-siskin-172863`
- Admin permissions to create service accounts and manage IAM
- Admin access to the GitHub repository to add secrets

## Steps

### 1. Create a Service Account

1. Go to the [GCP Console](https://console.cloud.google.com/)
2. Select project `gradient-pacs-siskin-172863`
3. Navigate to **IAM & Admin** > **Service Accounts**
4. Click **Create Service Account**
5. Fill in the details:
   - **Service account name**: `github-actions-test-runner`
   - **Service account ID**: `github-actions-test-runner`
   - **Description**: `Service account for running tests in GitHub Actions`
6. Click **Create and Continue**

### 2. Grant Required Permissions

Grant the service account scoped, least-privilege access. The only project-level role is `Service Usage Consumer`; storage access is bucket-scoped so a leaked key can't reach the rest of the project.

```bash
SA=github-actions-test-runner@gradient-pacs-siskin-172863.iam.gserviceaccount.com

# Project-level: only what's needed for quota_project_id (serviceusage.services.use).
gcloud projects add-iam-policy-binding gradient-pacs-siskin-172863 \
  --member=serviceAccount:$SA \
  --role=roles/serviceusage.serviceUsageConsumer

# Bucket-level: read/write on the two test buckets.
gcloud storage buckets add-iam-policy-binding gs://siskin-172863-test-data \
  --member=serviceAccount:$SA --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding gs://siskin-172863-temp \
  --member=serviceAccount:$SA --role=roles/storage.objectAdmin

# Bucket-level: read-only on the dicomweb fixtures bucket.
gcloud storage buckets add-iam-policy-binding gs://siskin-172863-pacs \
  --member=serviceAccount:$SA --role=roles/storage.objectViewer
```

Do **not** grant project-level `Storage Object Admin` or BigQuery roles. Tests don't need them, and the broader scope expands the blast radius if the key leaks.

### 3. Create and Download JSON Key

1. In the service accounts list, find `github-actions-test-runner`
2. Click the three dots menu (⋮) > **Manage keys**
3. Click **Add Key** > **Create new key**
4. Select **JSON** as the key type
5. Click **Create**
6. The JSON key file will be downloaded to your computer
7. **Keep this file secure** - it provides full access to the service account's permissions

### 4. Add GitHub Secrets

The secret must be set in **both** the Actions and Dependabot scopes so that dependabot PRs can run tests too. (GitHub does not share Actions secrets with dependabot by default.)

```bash
gh secret set GCP_SA_KEY --app actions    --repo gradienthealth/cloud_optimized_dicom < /path/to/key.json
gh secret set GCP_SA_KEY --app dependabot --repo gradienthealth/cloud_optimized_dicom < /path/to/key.json

# Wipe the local copy
rm -P /path/to/key.json   # or: shred -u /path/to/key.json on Linux
```

Equivalent UI path: **Settings** > **Secrets and variables** > **Actions** (and again under **Dependabot**), name `GCP_SA_KEY`.

### 5. Verify Setup

1. Push a commit or open a pull request
2. The `tests` workflow should run automatically
3. Check the workflow run in the **Actions** tab
4. Verify that authentication succeeds and tests run with `SISKIN_ENV_ENABLED=1`

## Security Notes

- **Never commit the service account JSON key to the repository**
- The key is only stored in GitHub Secrets (encrypted at rest)
- Rotate the key periodically for security
- If the key is compromised, delete it immediately from the GCP Console and create a new one

## Troubleshooting

### Authentication fails with "Could not automatically determine credentials"

- Verify that the `GCP_SA_KEY` secret is set correctly
- Ensure the secret contains the full JSON key (including opening and closing braces)
- Check that the service account still exists and hasn't been deleted

### Tests fail with permission errors

If you see `serviceusage.services.use access to the Google Cloud project` error:
- The service account is missing the **Service Usage Consumer** role
- Go to IAM & Admin > IAM, find the service account, and add this role

For other permission errors:
- Verify the bucket-level bindings from step 2: `objectAdmin` on `siskin-172863-test-data` and `siskin-172863-temp`, `objectViewer` on `siskin-172863-pacs`.
- Ensure all three buckets exist.
- If a new test reads from a bucket not listed above, add it to step 2 rather than re-granting project-level access.

### Tests are skipped

- Verify that `SISKIN_ENV_ENABLED=1` is set in the workflow file
- Check that the environment variable is being passed correctly to the test command
