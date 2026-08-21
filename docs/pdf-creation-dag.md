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
5. run_tesseract - run tesseract against the document's file list and selected languages to create the searchable PDF
6. validate_and_publish - confirm PDF successfully created, then move and rename to the final destination
7. cleanup - remove the temp directory


summarize_results:
1. Produce a final summary of the DAG run, including successful, failed, and skipped PDFs and enough information to identify failed documents.
---

### 2. Non-Python/Airflow dependencies that we’ll need. The big one is Tesseract, but there may be others. (The existing script uses ImageMagick, but we could replace it with another library if we prefer.)      

### Current Perl version:
ExifTool executable - Inspects image source for width, height, resolution   
ImageMagick executable - Resizes image (if necessary), normalizes format (to TIFF, if necessary) and compresses the temporary TIFF  
Tesseract executable - Searchable PDF Creation   

### Possible Mokelumne version:
libvips system library - perform inspection, resize, normalize and compress images (as necessary)  
pyvips Python package - interface for libvips  
Tesseract executable - Searchable PDF creation  
Before committing to this change, benchmark libvips/pyvips against the current ExifTool/ImageMagick workflow using representative TIFF and JPEG files. Compare processing time, memory usage, temporary disk usage, output size/quality, and compatibility with the image formats currently encountered.


---

### 3. A proposal for how the steps will run; i.e. the types of operators we’ll use. The Airflow @task decorator/PythonOperator will probably suitable for most of the logic/file manipulation, but we should have a plan for how we’ll actually run Tesseract. This intersects with (1) because we’ll need to think through how dependencies are deployed. 

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
  * `run_tesseract`- we'll want to determine best option during prototyping (consider simplicity, error handling, maintainability, etc...)
    * Option 1 - Invoke Tesseract using Python's `subprocess.run()` from within `process_document`.
    * Option 2 - Make Tesseract its own `@task.bash` task (Airflow executes Tesseract directly).
    * Option 3 - REST call to internal media/OCR service
  * `validate_and_publish` - Verify the generated PDF and publish to destination directory.
  * `cleanup`- Remove temporary workspace.

summarize_results
* Implemented as an Airflow `@task`.
* Runs after all mapped document processing has finished
* Generate final summary information for the Dag run

---

### 4. Make sure that the process does not run as single-threaded. For example, given a set of subdirectories each containing a batch of images and a manifest/filelist to turn into a PDF, use Airflow’s dynamic task mapping to generate a task for each subdirectory.

See `process_document` references above.

---
### 5. Any concerns, gotchas, or things we don’t understand.

* **Tesseract deployment:** I'm not sure if we have a preferred deployment setup for such external executables. Tesseract (and its trained language data) will need to be available in the Airflow worker container, I'm assuming at least.

* **Image-processing dependency choice:** ExifTool/ImageMagick vs libvips/pyvips - we should verify performance and compatibility using TIFF/JPEG inputs before we commit to an option.

* **Concurrency Limit:** How many PDFs can we process at once before one of CPU, memory, temporary disk space (other things?) become a bottleneck.

* **Temporary Storage:** Per comment in Perl script "/tmp has filled up on me in the past and ImageMagick isn't very good about deleting its HUGE temp files." Parallel could make that more likely so temp-dir location/cleanup need to be considered with that in mind.

* **Alma Failure Behavior:** The Perl script falls back to default languages if the Alma lookup fails, do we wish to preserve that behavior or fail the document?

* **Shared filesystem bottlenecs:** Even if CPU/memory are non-issues, reading many large TIFFs from PA or DA and writing PDFs back may cause bandwidth bottlenecks. Before defining the level of concurrent processing we should benchmark against storage performance.

* **Tesseract language availability:** We should define which language packs are required and make sure they are consistently available to avoid preventable failures.

* **Granularity:** Something worth considering, for process_document, I've proposed one mapped task per document with internal steps implemented as helper functions in order to keep task count manageable. That gives less Airflow UI visibility than making each step its own task. My thought is if we have 5,000 subdirectories, do we want 

* **Dynamic task mapping size:** I've read that Airflow has a configurable `max_map_length` for mapped task inputs; large batches of several thousand subdirectories may require increasing that setting or running the work in batches

* **Run batching vs. one giant map:** Increasing `max_map_length` could be a quick solve if we encounter a run that contains a large number of documents; but we could also consider chunking into batches if that's deemed cleaner.


---

### Tesseract execution and deployment options

**Option A - Dedicated Celery OCR Worker(s):** Create one or more persistent Airflow workers with Tesseract, trained language models and image-processing dependencies configured. These could listen on a dedicated "OCR" queue and can be configured to perform multiple PDF conversions concurrently (we'd want to do some testing to see what type of limitations we have before running into any CPU/memory issues)

**Option B - Docker Swarm OCR containers:** Use DockerSwarmOperator to spin up a temporary OCR service container for each document. The OCR image would contain Tesseract, language data, libvips, etc... Airflow/Swarm concurrency controls would then limit how many services run concurrently.

**Option C – Internal OCR web service:** Deploy Tesseract and associated language models(and potentially other image/video processing dependencies) as a separate web service accessible to Mokelumne over the private Docker overlay network. Instead of invoking Tesseract directly from an Airflow worker, `process_document` would make a REST call to the service and pass the information needed to process the document. This keeps large dependencies out of the Mokelumne worker image and could provide a reusable media-processing service for other workflows. We would still need to determine how the service accesses source/destination storage, how concurrency is managed, and if long-running jobs should use synchronous or asynchronous requests (I'd guess asynchronous, but I'm not sure how long these jobs take).


Shared storage access: Regardless of which option we choose, the OCR process will need access to source files on /srv/pa and /srv/da and will need to write the completed PDF to the appropriate destination. Direct access to the mounted storage would avoid unnecessarily copying large image files between Airflow and the OCR process. We will need to determine how this storage is made available to dedicated workers, temporary Swarm containers, or the web service, including any node availability, permissions, and security considerations.