# Publishing to PyPI

This repository includes a GitHub Action workflow for publishing the `cloud-optimized-dicom` package to PyPI.

## Setup (One-Time)

### 1. Configure PyPI Trusted Publishing

The workflow uses PyPI's "Trusted Publishing" feature, which is more secure than using API tokens. This allows GitHub Actions to publish directly to PyPI without storing secrets.

#### For PyPI:

1. Go to https://pypi.org/manage/account/publishing/
2. Scroll to "Add a new pending publisher"
3. Fill in:
   - **PyPI Project Name**: `cloud-optimized-dicom`
   - **Owner**: Your GitHub username or organization
   - **Repository name**: `cloud_optimized_dicom` (or your repo name)
   - **Workflow name**: `publish.yml`
   - **Environment name**: `pypi`
4. Click "Add"

#### For Test PyPI (Optional but Recommended):

1. Go to https://test.pypi.org/manage/account/publishing/
2. Follow the same steps as above, but use environment name `testpypi`

### 2. Create GitHub Environments

1. Go to your repository on GitHub
2. Click **Settings** → **Environments**
3. Create two environments:
   - **pypi** (for production releases)
   - **testpypi** (for testing)
4. Optionally, add protection rules to `pypi` environment:
   - Required reviewers (recommended)
   - Deployment branches: Only protected branches or tags

## Usage

### Method 1: Manual Trigger (Recommended for Testing)

1. Go to **Actions** tab in your GitHub repository
2. Click on **Publish to PyPI** workflow
3. Click **Run workflow**
4. Select:
   - **Branch**: usually `main`
   - **Target environment**: `testpypi` (for testing) or `pypi` (for production)
5. Click **Run workflow**

### Method 2: Automatic on Version Tags

When you're ready to publish a new version:

1. Update the version in `pyproject.toml`
2. Commit the change:
   ```bash
   git add pyproject.toml
   git commit -m "Bump version to 0.2.0"
   ```
3. Create and push a version tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
4. The workflow will automatically build and publish to PyPI

## First-Time Publishing

For your first publish, you MUST use the manual trigger method:

1. First, publish to **Test PyPI** to verify everything works
2. Test installing from Test PyPI:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ cloud-optimized-dicom
   ```
3. If successful, publish to **PyPI** (production)

After the first successful publish, the project will exist on PyPI and you can use either method.

## Troubleshooting

### "Project does not exist" error
- For first publish: The project name must exactly match what's in `pyproject.toml` (`cloud-optimized-dicom`)
- Make sure you've configured Trusted Publishing as described above

### Permission denied
- Verify the GitHub environment names match exactly: `pypi` and `testpypi`
- Check that Trusted Publishing is configured correctly on PyPI

### Build failures
- Check the build logs in the Actions tab
- Ensure `pyproject.toml` is valid and all required fields are present
