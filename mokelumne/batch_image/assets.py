"""Asset manager for the Batch Image workflow."""

from airflow.sdk import AssetAlias

records_xml = AssetAlias('mokelumne/batch-image/records.xml')
"""The XML output of the user's TIND query, containing all matching records.

Emitted by: `fetch_tind_records`
Consumed by: `filter_records`
"""

to_process_csv = AssetAlias('mokelumne/batch-image/to_process.csv')
"""The CSV file containing all of the TIND records that require processing.

Emitted by: `filter_records`
Consumed by: `fetch_images`
"""

skipped_csv = AssetAlias('mokelumne/batch-image/skipped.csv')
"""The CSV file containing all of the TIND records that were skipped/filtered.

Emitted by: `filter_records`
Consumed by: `notify_user`
"""

fetched_csv = AssetAlias('mokelumne/batch-image/fetched.csv')
"""The CSV file containing the TIND records and their fetching status.

Emitted by: `fetch_images`
Consumed by: `generate_image_descriptions`
"""

processed_csv = AssetAlias('mokelumne/batch-image/processed.csv')
"""The CSV file containing all processed TIND records and generated descriptions.

Emitted by: `generate_image_descriptions`
Consumed by: `notify_user`
"""

failed_csv = AssetAlias('mokelumne/batch-image/failed.csv')
"""The CSV file containing TIND records that were due to be processed, but
encountered one or more failures during processing.

Emitted by: `generate_image_descriptions`
Consumed by: `notify_user`
"""

public_dir = AssetAlias('mokelumne/batch-image/public-dir')
"""The publicly accessible directory where user-facing assets are stored.

Emitted by: `summarise_job`
Consumed by: `notify_user`
"""
