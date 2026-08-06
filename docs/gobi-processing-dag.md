# GOBI processing DAG plan

## Goal

Replace the continuously running Ruby GOBI watcher with an Airflow 3.3.0 Dag
that preserves the current processing contract:

1. Find incoming files whose names end in `.ord`.
2. Read binary MARC21 records and take the provider from the first three
   characters of `961$d`.
3. Route a missing or unsupported provider to `ZZZ`.
4. Write one output per provider using the existing
   `ebook<PROVIDER><YEAR>...ord` filename convention.
5. Move the original order file to the processed directory only after all of
   its records have been handled.

## Dag design

`process_gobi_orders` runs every two minutes by default and does not catch up
missed intervals. The schedule can be changed with
`MOKELUMNE_GOBI_SCHEDULE`. `max_active_runs=1` prevents scheduled or manual Dag
runs from scanning the same shared directory concurrently.

The Dag contains two Task SDK tasks:

1. `discover_order_files` scans the configured incoming directory at task
   runtime and returns a sorted list of `.ord` files.
2. `process_one_order_file` is dynamically mapped over that list, with at most
   four mapped task instances running at once. Each task uses `pymarc` to split
   one input file and then archives it.

The paths are Dag parameters so an operator can override them for a manual
run. Their deployment defaults come from:

- `MOKELUMNE_GOBI_INPUT_DIR`
- `MOKELUMNE_GOBI_OUTPUT_DIR`
- `MOKELUMNE_GOBI_PROCESSED_DIR`

All three directories must already exist and must be mounted at the same paths
on every Airflow worker. The Compose defaults live below
`/opt/airflow/files/gobi`.

## Failure and retry behavior

Provider files are written to hidden temporary files in the output directory.
They are renamed to their final names only after the complete source file has
parsed and written successfully. A malformed MARC record therefore leaves the
input in place and publishes no partial output.

If a provider output already exists, it is not overwritten. This preserves the
Ruby processor's behavior and makes a retry safe if outputs were published but
the task failed before archiving the input. The task also refuses to overwrite
an existing archived source file.

## Implementation and rollout

1. Add isolated helpers in `mokelumne.util.gobi` and unit-test provider
   extraction, naming, discovery, splitting, existing-output behavior, and
   malformed-input cleanup.
2. Add the `airflow.sdk` Dag and a structure test for its schedule-safety,
   mapping tasks, and parameters.
3. Mount or create the three configured directories on every Celery worker.
4. Test a copied production `.ord` file in non-production directories and
   compare provider names and MARC record counts with the Ruby output.
5. Pause the Ruby watcher, deploy the initially paused Dag, and run one manual
   smoke test.
6. Confirm Alma can consume the generated files, then unpause the Dag. Keep the
   Ruby container available for rollback until the Airflow run history is
   stable.
