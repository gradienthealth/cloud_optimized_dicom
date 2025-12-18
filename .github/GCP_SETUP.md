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

Grant the service account access to Google Cloud Storage and Service Usage:

1. Click **Select a role**
2. Add the following roles:
   - **Storage Object Admin** - Allows creating, reading, and deleting objects in GCS
   - **Storage Bucket Reader** - Allows listing buckets
   - **Service Usage Consumer** - Required for quota_project_id usage (provides serviceusage.services.use permission)
3. Click **Continue**, then **Done**

### 3. Create and Download JSON Key

1. In the service accounts list, find `github-actions-test-runner`
2. Click the three dots menu (⋮) > **Manage keys**
3. Click **Add Key** > **Create new key**
4. Select **JSON** as the key type
5. Click **Create**
6. The JSON key file will be downloaded to your computer
7. **Keep this file secure** - it provides full access to the service account's permissions

### 4. Add GitHub Secret

1. Go to your GitHub repository
2. Navigate to **Settings** > **Secrets and variables** > **Actions**
3. Click **New repository secret**
4. Fill in the details:
   - **Name**: `GCP_SA_KEY`
   - **Value**: Paste the entire contents of the downloaded JSON key file
5. Click **Add secret**

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
- Verify the service account has all three required roles (Storage Object Admin, Storage Bucket Reader, Service Usage Consumer)
- Ensure the test buckets (`siskin-172863-test-data`, `siskin-172863-temp`) exist
- Check that the service account has access to these specific buckets

### Tests are skipped

- Verify that `SISKIN_ENV_ENABLED=1` is set in the workflow file
- Check that the environment variable is being passed correctly to the test command
