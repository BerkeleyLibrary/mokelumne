# SFTP fetch Dags

Mokelumne has two independent workflows for replacing the legacy
`sftp_handler` cron jobs:

| Dag | Schedule | Remote file | Default destination |
| --- | --- | --- | --- |
| `fetch_gobi_order_file` | Daily at 5:00 AM America/Los_Angeles | `/gobiord/ebookMMDD.ord` | `/srv/alma/gobi-ebook-eocr-input` |
| `fetch_lbnl_patron_file` | Tuesdays at 1:00 AM America/Los_Angeles | `lbnl_people_YYYYMMDD.zip` for the most recent Monday | `/srv/alma/patron_lbl` |

Both Dags are paused when first created, do not catch up missed intervals, and
allow only one active run. They are not connected to other Dags. In particular,
the existing `process_gobi_orders` Dag continues to discover files on its own
schedule after the fetch Dag publishes them to its input directory.

## Connections and secrets

Create `gobi_sftp` and `lbnl_sftp` Airflow connections, or change their IDs
with `MOKELUMNE_GOBI_SFTP_CONN_ID` and `MOKELUMNE_LBNL_SFTP_CONN_ID`.
The GOBI connection uses password authentication and the LBNL connection uses
a private key. Store those credentials in the approved deployment secrets
system, never in this repository.

Both connections must explicitly set `no_host_key_check` to `false`. Configure
a pinned `host_key`, or provision a trusted known-hosts file on every worker.
The task rejects a connection that does not explicitly enable verification.
If `key_file` is used for LBNL, mount the key at the same path and with
permissions readable by the Airflow runtime UID on every Celery worker.

The destination directories must already exist, must not be symlinks, and must
be mounted at the same paths on every worker. Override the defaults with:

* `MOKELUMNE_GOBI_DOWNLOAD_DIR`
* `MOKELUMNE_LBNL_DOWNLOAD_DIR`
* `MOKELUMNE_LBNL_FILENAME_PREFIX`

## Download behavior

Each task checks for a prior local download before opening an SFTP connection.
LBNL also treats `<filename>*.old` as evidence that Alma already processed the
file. An expected missing remote file, a stale scheduled GOBI file, or a prior
local download marks the task skipped.

Downloads are written to a unique hidden temporary file in the destination
directory. The task compares its size with the remote metadata, then publishes
it atomically without overwriting any path that appeared concurrently. Failed
transfers remove their temporary files and retry twice at fifteen-minute
intervals.

Scheduled GOBI runs reject files modified more than ten days earlier to avoid
retrieving a prior year's same-`MMDD` file. A manually supplied `filename`
explicitly requests a historical file and bypasses that freshness check. LBNL
manual filenames must use the configured prefix and a valid `YYYYMMDD` date.

## Production cutover

Configure the connections, secrets, volumes, and environment on every worker
before unpausing either Dag. Disable the matching legacy cron service
immediately before enabling its replacement to prevent duplicate writers.
Observe the first scheduled run and verify the published file's ownership and
permissions before retiring the legacy service permanently.
