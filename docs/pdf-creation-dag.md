# PDF Creation DAG Architecture

## Overview

This document proposes an Airflow implementation to replace the existing Perl-based PDF creation tool. It is intended as a discussion document to help define the overall workflow, dependencies, and areas requiring further investigation before implementation.

## 1. A breakdown of the steps (name the tasks and summarize what they do).

validate_inputs:
1. Validate the source and destination paths.   
2. Validate source directory structure.   
3. Confirm source contains TIFF/JPEG document directories. 

discover_documents:
1. Scan parent source directory and build the list of subdirectories to process
2. Build a list of document work items containing the source subdirectory and expected output PDF filename. This list will be used for Dynamic Task Mapping.

process_document: (dynamically mapped Python task)
Each discovered document (subdirectory) is processed by one mapped Airflow task. Multiple instances may run concurrently (will depend on Airflow limits).
Steps the task will perform:
1. check_output - checks if the PDF already exists and applies skip/overwrite behavior
2. prepare_workspace - create the run/document specific temp directory
3. determine_languages - use default Tesseract languages or, if subdirectory begins with an MMS ID, retrieve the MARC record through Alma provider and map its languages to Tesseract language models
4. prepare_images - sort source TIFF/JPEG images in page order, inspect resolution, resize where appropriate, normalize to TIFF and create the ordered Tesseract file list
5. submit_ocr_job - submit the prepared file list, selected language(s), and output path to the OCR web service and receive a job ID/status URL
6. wait_for_ocr - monitor the OCR job until it succeeds or fails
7. validate_and_publish - confirm the PDF was successfully created, verify the returned checksum, then move/rename it to the final destination
8. cleanup - remove the temp directory


summarize_results:
1. Produce a final summary of the DAG run, including successful, failed, and skipped PDFs and enough information to identify failed documents.
---

### 2. Non-Python/Airflow dependencies that we’ll need. The big one is Tesseract, but there may be others. (The existing script uses ImageMagick, but we could replace it with another library if we prefer.)      

### Current Perl version:
ExifTool executable - Inspects image source for width, height, resolution   
ImageMagick executable - Resizes image (if necessary), normalizes format (to TIFF, if necessary) and compresses the temporary TIFF  
Tesseract executable - Searchable PDF Creation   

### Proposed architecture:
Mokelumne:
- Python image-preparation dependencies such as libvips/pyvips
- OCR provider/client used to communicate with the OCR service

OCR service:
- Flask
- Tesseract 5 executable
- pytesseract Python wrapper
- trained Tesseract language data
- queue/worker infrastructure
- persistent job-status storage


---

### 3. A proposal for how the steps will run; i.e. the types of operators we’ll use. The Airflow @task decorator/PythonOperator will probably be suitable for most of the logic/file manipulation, but we should have a plan for how we’ll actually run Tesseract. This intersects with (1) because we’ll need to think through how dependencies are deployed. 

validate_inputs
* Implemented as an Airflow `@task` (Python task).
* Performs filesystem validation and dependency checks.
* Fails the Dag immediately if the batch is invalid.

discover_documents
* Implemented as an Airflow `@task`.
* Discovers eligible document directories.
* Builds the collection of document work items that will be used for Dynamic Task Mapping.

process_document
* Implemented as a dynamically mapped Airflow `@task`
* One mapped task is created for each discovered document/subdirectory.
* The internal document-processing steps will be implemented as normal Python helper functions:
  * `check_output` - Determines whether the destination PDF already exists.
  * `prepare_workspace` - Creates the temporary working directory.
  * `determine_languages` - Uses a to be created Alma provider (when applicable) to determine OCR languages.
  * `prepare_images` - Uses libvips (or ImageMagick) to inspect, resize, normalize and prepare images.
  * `submit_ocr_job`
    * Uses an Airflow OCR provider/client to make a REST request to the internal OCR service.
    * Sends the file list/manifest location, selected language(s), and desired output path.
    * Expects a `202 Accepted` response containing a job ID/status URL.

  * `wait_for_ocr`
    * Uses a deferrable operator/sensor to monitor the OCR job without occupying a worker slot while waiting.
    * Fails the mapped document task if the OCR service reports failure.
  * `validate_and_publish` - Verify the generated PDF and publish to destination directory.
  * `cleanup` - Remove temporary workspace.

summarize_results
* Implemented as an Airflow `@task`.
* Runs after all mapped document processing has finished
* Generate final summary information for the Dag run

---

### 4. Make sure that the process does not run as single-threaded. For example, given a set of subdirectories each containing a batch of images and a manifest/filelist to turn into a PDF, use Airflow’s dynamic task mapping to generate a task for each subdirectory.

See `process_document` references above.

---
### 5. Any concerns, gotchas, or things we don’t understand.

* **Tesseract deployment:** Deploy Tesseract in a separate OCR service.

* **Image-processing dependency choice:** ExifTool/ImageMagick vs libvips/pyvips - we should verify performance and compatibility using TIFF/JPEG inputs before we commit to an option.

* **Concurrency Limit:** How many PDFs can we process at once before one of CPU, memory, temporary disk space (other things?) become a bottleneck. Note - partly owned by the OCR service worker/queue architecture

* **Temporary Storage:** Per comment in Perl script "/tmp has filled up on me in the past and ImageMagick isn't very good about deleting its HUGE temp files." Parallel could make that more likely so temp-dir location/cleanup need to be considered with that in mind.

* **Alma Failure Behavior:** The Perl script falls back to default languages if the Alma lookup fails, do we wish to preserve that behavior or fail the document?

* **Shared filesystem bottlenecks:** Even if CPU/memory are non-issues, reading many large TIFFs from PA or DA and writing PDFs back may cause bandwidth bottlenecks. Before defining the level of concurrent processing we should benchmark against storage performance.

* **Tesseract language availability:** Trained language data will probably live on a dedicated volume available to the OCR service.

* **Granularity:** `process_document` is proposed as one mapped task per document, with internal steps implemented as helper functions. This keeps the Airflow task count manageable for runs containing thousands of documents, but provides less task-level visibility in the UI than splitting every internal step into a separate Airflow task.

* **Dynamic task mapping size:** I've read that Airflow has a configurable `max_map_length` for mapped task inputs; large batches of several thousand subdirectories may require increasing that setting or running the work in batches

* **Run batching vs. one giant map:** Increasing `max_map_length` could be a quick solve if we encounter a run that contains a large number of documents; but we could also consider chunking into batches if that's deemed cleaner.

* **OCR service availability/failure handling:**
* **job status persistence:**
* **orphaned/dead jobs:**
* **API contract/versioning:**
* **checksum verification:**

---

### Tesseract execution and deployment options

**Preferred direction – Internal OCR web service:** Deploy Tesseract and its trained language data in a separate Flask-based service accessible to Mokelumne over the private Docker overlay network. Mokelumne will submit OCR jobs over REST rather than invoking Tesseract directly. The service will use `pytesseract` to invoke the Tesseract executable and will process jobs asynchronously through worker processes.

`POST /jobs` will validate and persist the request, enqueue the work, and return `202 Accepted` with a job ID/status URL. Mokelumne will poll `GET /jobs/:jobid`, likely through a deferrable Airflow operator, until the job succeeds or fails. Successful job responses should include the output PDF path and checksum.

### Alternatives considered
**Option A - Dedicated Celery OCR Worker(s):** Create one or more persistent Airflow workers with Tesseract, trained language models and image-processing dependencies configured. These could listen on a dedicated "OCR" queue and can be configured to perform multiple PDF conversions concurrently (we'd want to do some testing to see what type of limitations we have before running into any CPU/memory issues)

**Option B - Docker Swarm OCR containers:** Use DockerSwarmOperator to spin up a temporary OCR service container for each document. The OCR image would contain Tesseract, language data, libvips, etc... Airflow/Swarm concurrency controls would then limit how many services run concurrently.

Shared storage access: Regardless of which option we choose, the OCR process will need access to source files on /srv/pa and /srv/da and will need to write the completed PDF to the appropriate destination. Direct access to the mounted storage would avoid unnecessarily copying large image files between Airflow and the OCR process. We will need to determine how this storage is made available to dedicated workers, temporary Swarm containers, or the web service, including any node availability, permissions, and security considerations.
