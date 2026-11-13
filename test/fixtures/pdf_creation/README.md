# PDF creation fixtures

Use these Dag Params in the local Docker Compose environment:

```json
{
  "source": "/opt/airflow/test/fixtures/pdf_creation/inputs",
  "destination": "/opt/airflow/test/fixtures/pdf_creation/outputs",
  "language": "eng",
  "max_resolution": 200
}
```

Keep `pdf_creation_scratch_base` on storage mounted at the same path in the
Airflow workers and Quiabo. Only the prepared images and file list need to be
accessible to Quiabo; the source and final destination are used by Airflow.

| Document | Expected outcome |
| --- | --- |
| `successful` | Readable text image; publish `outputs/successful.pdf`. |
| `tesseract-failure` | Valid 32,768 x 32 pixel TIFF; record `OCR_JOB_FAILED`. |
| `skipped` | Find the committed `outputs/skipped.pdf`; record `PDF_ALREADY_EXISTS`. |

Each input directory contains three pages named `{dirname}-000.tif` through
`{dirname}-002.tif`. All source TIFFs are 150 DPI. The oversized images stay above
Tesseract's
32,767-pixel dimension limit after preprocessing. Verified with Tesseract 5.5.0:
it exits with code 1 and `Image too large: (32768, 32)`.

The overall Dag run is expected to fail because of the intentional Tesseract
error. Skips are acceptable when at least one document is discovered and no
documents fail. An all-skipped run succeeds; an empty run fails. The HTML summary
should contain all three document outcomes.

Generated outputs are ignored by Git; `skipped.pdf` is deliberately retained as
a tracked fixture. Before repeating the run, remove the generated PDFs so the
successful document is processed again:

```sh
rm -f test/fixtures/pdf_creation/outputs/successful.pdf \
      test/fixtures/pdf_creation/outputs/tesseract-failure.pdf
```

Keep `outputs/skipped.pdf` in place. Restore it with `git restore` if modified
after these fixtures have been committed.

## Live OCR test

Tests marked `live_ocr` are skipped by default, including in CI. Opt in explicitly:

```sh
docker compose exec airflow-cli python -m pytest --run-live-ocr -m live_ocr
```

The `api_client` fixture logs in with the local `testadmin:testadmin` credentials
configured by `airflow-init`. It uses `http://airflow-apiserver:8080` from the
test container; set `PYTEST_AIRFLOW_API_BASE_URL=http://localhost:8080` when
running pytest from the host.

List these tests without running them:

```sh
docker compose exec airflow-cli python -m pytest --collect-only -m live_ocr
```
