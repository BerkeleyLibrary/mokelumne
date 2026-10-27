# PDF Cleanup Dag

## Overview

The `pdf_creation_cleanup` Dag removes temporary working directories left behind by completed runs of the `pdf_creation` Dag.

PDF creation can require substantial temporary disk space for image preparation and OCR processing. These working directories may remain after a Dag run finishes, so periodic cleanup helps prevent unnecessary disk usage.

The cleanup Dag identifies successful or failed PDF creation runs within a configurable age range (30–90 days by default) and removes their associated temporary directories when deletion is enabled.

The Dag is triggered manually (`schedule=None`) and runs in dry-run mode by default, allowing operators to review eligible directories before enabling deletion.

To reduce the risk of accidental data loss, the Dag validates directory paths before removing anything.

## Dag Parameters
The `pdf_creation_cleanup` Dag accepts three parameters when triggered manually:

| Parameter | Default | Description |
| --------- | ------- | ----------- |
| `cleanup_window` | 30 | Number of days a `pdf_creation` Dag run must be older than to qualify for cleanup. |
| `max_days` | 90 | Maximum number of days to look back when searching for completed PDF creation runs. Must be greater than `cleanup_window`. |
| `dry_run` | True | When enabled, identifies and validates eligible directories without deleting them. Set to `False` to perform the cleanup. |

With the default settings (`cleanup_window=30` and `max_days=90`), the Dag considers successful or failed PDF creation runs that completed between 30 and 90 days ago. Runs completed within the last 30 days are retained, while runs older than 90 days are excluded from the search.

Both date boundaries are applied through the Airflow REST API to avoid retrieving unnecessary historical runs. To clean up older runs, increase `max_days` when manually triggering the Dag.

For safety, `dry_run` defaults to `True`. Operators should review the eligible directories before triggering a run with deletion enabled.

## Configuration

The `pdf_creation_cleanup` Dag requires an Airflow Variable and an Airflow Connection.

### Airflow Variable

- **`pdf_creation_scratch_base`** — The base directory containing temporary working directories created by the `pdf_creation` Dag. The cleanup process uses this location and the eligible Dag run IDs to identify directories for removal.

This Variable must point to the same scratch directory used by the PDF Creation workflow.

### Airflow Connection

- **`airflow_api`** — An HTTP Connection containing the credentials and endpoint used to access Airflow's REST API.

The Connection requires:

| Field | Description |
|-------|-------------|
| Connection ID | `airflow_api` |
| Connection Type | `HTTP` |
| Host | Airflow API server URL |
| Port | Airflow API server port |
| Login | API service account username |
| Password | API service account password |

For local development, Docker Compose creates the `pdf_cleanup_service` account with the Viewer role and configures the `airflow_api` Connection during `airflow-init`, provided `PDF_CLEANUP_API_PASSWORD` is set.
Set this variable in the local `.env` file before running `airflow-init`. It is optional for general local development but required to run the PDF Cleanup Dag.

Staging and production environments must provide an equivalent Connection and service account with appropriate permissions.

## Airflow API Integration
### Authenticate
Use credentials from the `airflow_api` Connection to request an authentication token from `/auth/token`.

### Retrieve and Filter Dag Runs
Query `/api/v2/dags/pdf_creation/dagRuns` using `end_date_gte` and `end_date_lte` to restrict results to the configured cleanup window. Follow pagination and select successful or failed runs within that window.

### Locate Scratch Directories
Retrieve the scratch directory recorded in XCom for each eligible run, then validate that it belongs within the configured scratch base.

## Workflow

The `pdf_creation_cleanup` Dag consists of five tasks executed sequentially:

1. **`calculate_cutoff`** — Calculates the cutoff and lookback dates using `cleanup_window` and `max_days`. Validates that `max_days` is greater than `cleanup_window`.

2. **`find_eligible_runs`** — Queries the Airflow REST API for `pdf_creation` Dag runs completed within the configured date range and identifies successful or failed runs eligible for cleanup.

3. **`find_run_directories`** — Retrieves the recorded scratch directory for each eligible `pdf_creation` run from the `prepare_run_directory` task's XCom. Runs without a recorded directory are skipped.

4. **`validate_run_directories`** — Validates the candidate directory paths to ensure they are within the configured scratch directory and are safe to remove.

5. **`remove_run_directories`** — Revalidates each directory immediately before deletion. If `dry_run` is `True`, reports the eligible directories without deleting them. Otherwise, removes the validated directories.


## Safety Measures

Because the `pdf_creation_cleanup` Dag deletes directories from the filesystem, several safeguards are implemented to prevent accidental data loss:

- **Dry-run by default:** The `dry_run` parameter defaults to `True`, allowing operators to identify eligible directories without deleting anything.
- **Retention and lookback limits:** Only successful or failed `pdf_creation` Dag runs completed within the configured cleanup window are eligible. Running or otherwise incomplete runs are excluded. Configurations where `max_days` is less than or equal to `cleanup_window` are rejected.
- **Restricted deletion paths:** Candidate directories must be direct children of the configured `pdf_creation_scratch_base` directory, with directory names matching their Dag run IDs.
- **Path validation:** Directory paths are validated before deletion to prevent removing unexpected or unsafe locations.
- **Revalidation before removal:** Each directory is checked again immediately before deletion, reducing the risk of removing an unintended directory if its path or state has changed.
- **Missing directories:** Directories that no longer exist are skipped rather than treated as errors.


## Running and Testing

### Running the Dag

The `pdf_creation_cleanup` Dag is configured with `schedule=None` and must be triggered manually through the Airflow UI.

1. Open the Airflow UI and navigate to the `pdf_creation_cleanup` Dag.
2. Select **Trigger** to start a new Dag run.
3. Configure the parameters:
   - `cleanup_window`: Minimum age of completed runs eligible for cleanup (default: `30`).
   - `max_days`: Maximum age of completed runs to consider (default: `90`). Must exceed `cleanup_window`.
   - `dry_run`: Set to `True` to preview eligible directories without deleting them.
4. Trigger the Dag and review the task logs, particularly `find_eligible_runs`, `validate_run_directories`, and `remove_run_directories`.
5. Once the eligible directories have been reviewed, trigger the Dag again with `dry_run=False` to perform the cleanup.

**Important:** Deletion is permanent. Always verify the selected directories with `dry_run=True` first. After verification, switch to `False` to permanently delete the directories listed.


### Testing

Unit and Dag tests are provided for the cleanup workflow. With the local Docker Compose environment running, execute the focused tests using:

```bash
docker compose exec airflow-cli python3 -m pytest \
  test/dags/test_pdf_creation_cleanup.py \
  test/unit/test_pdf_cleanup_utils.py
```

These tests cover Dag structure, eligibility filtering, directory validation, and cleanup behavior.

To run the complete test suite:

```bash
docker compose exec airflow-cli python3 -m pytest
```
