# JPEG derivatives Dag

`make_jpeg_derivatives` replaces the Perl `mkderivs.pl` conversion workflow. It
uses Airflow 3.3 TaskFlow `@dag`/`@task` decorators and pyvips with native TIFF/JPEG
codecs. It runs
only when triggered, with no parameter defaults or production paths embedded in
the Dag.

## Triggering a run

Supply these required parameters in the Airflow trigger form or API:

```json
{
  "input_root": "/srv/example/source",
  "output_root": "/srv/example/derivatives",
  "log_name": "example"
}
```

The roots must be absolute, disjoint paths without `..` or symlinked directory
components. The source must exist and be readable. Missing destination
directories are created. `log_name` must begin with an ASCII letter or digit and
contain at most 64 ASCII letters, digits, underscores, or hyphens.

All Celery workers must see the same paths, manifests, reports, and outputs on
shared storage, with permissions suitable for the Airflow runtime UID. The
implementation targets the Linux Airflow container: native input reads use
`/proc/self/fd` to retain the exact source inode opened without following links.
The output filesystem must support hard links and directory/file `fsync`.
Validation probes hard-link publication; a failed probe stops the run. Nested
destination mounts must support the same operations.

Keep the source tree stable during a run. Discovery freezes source paths, sizes,
and modification times. Device IDs and inode numbers are not compared across
workers because filesystem identities can differ between mounts. Changed or missing sources are
reported as per-file errors. Later arrivals are processed by a new run.

## Workflow and image policy

The stages are validation, discovery, mapped conversion batches, report
finalization, and a completion guard. Only one run is active at a time. Discovery
uses a temporary on-disk SQLite catalog to sort paths and detect collisions, then
writes JSONL manifests on shared storage. Each batch targets 100 files; batches
grow as needed to remain within 128 mapped tasks. Ensure Airflow's
`[core] max_map_length` is at least 128 (the default is 1,024). Two conversion tasks
can run concurrently, with two libvips threads per task and the operation cache
disabled during conversion.

The Dag finds `.tif` and `.tiff` files case-insensitively at every depth, including
the root. It skips dot-prefixed filenames, follows ordinary hidden directories,
and excludes all symlinks, including broken links and directory links. Extensions
must match exactly; `page.tif.bak` is ignored. Empty directories are not mirrored.
The destination for `folder/page.TIFF` is `folder/page.jpg`. Collisions such as
`page.tif`/`page.tiff`, file/directory conflicts, and a source subtree named
`.mkderivs` fail discovery before conversion.

Conversion selects page zero only, without automatic rotation. The initial
supported image types are unsigned 8/16-bit grayscale and RGB, including bilevel
TIFFs expanded to eight-bit grayscale. Alpha channels, CMYK/Lab, floating-point
pixels, and other layouts become review errors without a published JPEG.

JPEGs use quality 70, progressive encoding, forced 4:2:0 chroma subsampling for
RGB, and stripped metadata. Grayscale JPEGs have no chroma channels. Recognized
16-bit grayscale/RGB values are reduced to eight bits by libvips' saver. There is
no DPI-based resizing: the Perl script retrieved resolution but did not use it.

Images exceeding 65,500 pixels on either axis are reduced proportionally with
Lanczos3. Integer rounding and the minimum one-pixel axis can affect extreme
aspect ratios. Images within the limit are not resized. A completed temporary
JPEG is header-checked before a same-directory hard link publishes it atomically
without replacement. Existing regular outputs are skipped without decoding or
checking their provenance. Symlinks and non-regular destination entries are
rejected. No overwrite option is provided.

## Reports, logs, and retries

Reports live under:

```text
<output_root>/.mkderivs/<log_name>/<sha256-of-Airflow-run-id>/
```

`run.json` stores the original run ID, parameters, and pyvips/libvips versions.
Manifests, exclusions, and batch result journals remain there for investigation.
Each batch checkpoints completed results with a flushed, synchronized JSONL
record. A task retry reuses successful checkpoints whose output is still a
regular file, retries per-file errors, and recovers a torn final journal line.
Existing output is preserved even if a worker died between publication and
checkpointing; the retry records that file as skipped. A new Dag run also skips
existing output and attempts outstanding TIFFs.

Finalization publishes:

| File | Contents |
| --- | --- |
| `progress.txt` | Deterministically ordered paths, conversion statuses, exclusions, warnings, and final counts. |
| `results.jsonl` | One final record per discovered TIFF with source/output paths, status, diagnostics, and available dimensions. |
| `errors.txt` | Source paths and diagnostics for per-file errors; absent when there are none. |
| `summary.json` | Parameters, library versions, counts, report paths, incomplete work, and completion state. |

Per-file statuses are `CONVERTED`, `DOWNSCALED`, `SKIPPED`, and `ERROR`. Exclusions
are listed separately in progress output. Unprocessed TIFFs have `INCOMPLETE`
placeholder results if a task fails. Summary states are `complete`,
`complete_with_warnings`, and `incomplete`. Counts distinguish converted,
downscaled, skipped, erroneous, excluded, and incomplete inputs plus warning messages.
An empty input is a successful no-op with a warning and portable reports.

Individual image failures, **even when every TIFF fails**, allow a successful
Dag with a `complete_with_warnings` summary. Inspect the reports: these cases do
not activate task-failure alerts. Validation, unsafe paths, naming conflicts,
storage operations, report failures, and unfinished batches fail the run.
Finalization runs after failed tasks to preserve whatever results are available;
the final completion guard also depends directly on all preceding stages so
report generation cannot hide a failed task. If validation itself fails before
the report directory is initialized, diagnostics are available in Airflow logs.
Infrastructure tracebacks remain in Airflow task logs; portable reports identify
incomplete work without copying those tracebacks.

Module-level Python logging supplies live progress and exceptions to Airflow.
Native VIPS warnings are collected through pyvips' Python logger, annotated with
the active source path, and included in portable results. Conversion inside each
mapped task is serial, including warning attribution. Python output callbacks
preserve filesystem write errors from the native encoder so disk failures stop
the task instead of becoming ordinary image errors.

Airflow's logs are split by task, mapped instance, and attempt and follow its
configured retention. Batch journals provide progress during execution; the
single consolidated progress report becomes available at finalization. Parallel
execution does not retain the Perl script's global chronological progress order.
The summary replaces its ambiguous `_FINISHED` filename. Airflow supplies elapsed
time; reports do not calculate it. Reports are retained until explicitly removed;
there is no automatic cleanup policy.

## Compatibility and operational verification

This preserves the legacy conversion intent, without guaranteeing identical
pixels, JPEG bytes, or file sizes:

- **JPEG encoding:** pyvips has no direct equivalent to ImageMagick's
  `jpeg:dct-method=float`. Native JPEG libraries and quantization can produce
  different artifacts even with quality 70 and the same subsampling.
- **Color:** stripping an ICC profile does not transform pixels to sRGB. Profiled
  RGB is converted with a recorded warning, preserving the legacy stripping
  policy. Appearance can differ in profile-aware viewers. CMYK/Lab needs review
  before support is added.
- **Transparency/depth:** libvips normally composites JPEG alpha onto black.
  Alpha inputs are therefore excluded. Float and other numeric layouts require
  a deliberate range policy; indiscriminate casts can clip tonal values.
- **Orientation/pages:** stored pixel orientation is retained and nontrivial
  orientation metadata produces a warning before stripping. This can leave a
  derivative sideways. Only the first TIFF page is selected.
- **Resampling:** Lanczos3 and dimension rounding can differ from the legacy
  ImageMagick build. Compare scanned text and oversized photographs visually.
- **TIFF codecs/damage:** support depends on native libtiff/libvips builds. Test
  actual collection compression types, tiled/striped TIFFs, and BigTIFF. Damaged
  pixels can fail only during JPEG encoding because decoding is lazy. Native
  decoder safety limits remain enabled; newer libtiff versions may reject large
  decoding allocations.
- **Memory:** a 65,500-square RGB image represents about 12.9 GB of unpacked
  pixels, before progressive JPEG coefficient buffers. Sequential access and
  cache/thread limits do not cap encoder memory. Benchmark representative large
  files and shared-storage throughput before increasing concurrency.

The project already declares pyvips and installs native `libvips42`. After
building and recreating the relevant Compose services, check the native version,
run `pip check`, and run focused helper and Dag tests followed by the complete
suite. Before operational use, compare representative collection inputs against
existing legacy JPEGs. Production collection data and legacy comparison results
are not included in this repository's regression fixtures.

References: [Airflow task logging](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/logging-monitoring/logging-tasks.html),
[TIFF loading](https://www.libvips.org/API/8.17/ctor.Image.tiffload.html),
[JPEG saving](https://www.libvips.org/API/8.17/method.Image.jpegsave.html),
[libvips resizing](https://www.libvips.org/API/current/method.Image.resize.html).
