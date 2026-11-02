"""Create retryable JPEG derivatives and portable batch reports on shared storage."""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import stat
import tempfile
import uuid
from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator, TextIO

from mokelumne.util.storage import run_dir

logger = logging.getLogger(__name__)

MAX_JPEG_DIMENSION = 65_500
MAX_BATCHES = 128
TARGET_BATCH_SIZE = 100
REPORT_DIRECTORY = ".mkderivs"
LOG_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
SUCCESS_STATUSES = {"CONVERTED", "DOWNSCALED", "SKIPPED"}


class ImageReviewError(ValueError):
    """An individual source cannot safely be converted under the image policy."""


@dataclass(frozen=True)
class Run:
    """Serializable run configuration, independent of Airflow context."""

    input_root: str
    output_root: str
    log_name: str
    run_id: str
    versions: dict[str, str]


@dataclass(frozen=True)
class WorkItem:
    """A relative source path and its discovery-time size and modification time."""

    relative: str
    size: int
    mtime_ns: int


@dataclass
class Result:
    """A durable per-file result, including native-library diagnostics."""

    # pylint: disable=too-many-instance-attributes

    relative: str
    source: str
    output: str
    status: str
    warnings: list[str] = field(default_factory=list)
    error: str | None = None
    input_size: list[int] | None = None
    output_size: list[int] | None = None


def _vips() -> Any:
    """Import the native bindings only when a task actually needs them."""
    # pylint: disable=import-outside-toplevel
    import pyvips  # type: ignore[import-untyped]

    return pyvips


def _absolute(path: str) -> Path:
    """Require an absolute path without parent traversal."""
    result = Path(path)
    if not path or not result.is_absolute() or ".." in result.parts:
        raise ValueError(f"Expected an absolute path without traversal: {path!r}")
    return result


@contextmanager
def _directory(path: Path, *, create: bool = False) -> Iterator[int]:
    """Open directory components without following links, optionally creating them."""
    path = _absolute(str(path))
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            if create:
                try:
                    os.mkdir(part, dir_fd=descriptor)
                except FileExistsError:
                    pass
            child = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        yield descriptor
    finally:
        os.close(descriptor)


def _regular_at(directory: int, name: str) -> bool:
    """Return whether an entry exists, rejecting links and non-regular entries."""
    try:
        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"Unsafe non-regular file: {name!r}")
    return True


@contextmanager
def _reader(path: Path) -> Iterator[TextIO]:
    """Read a regular UTF-8 file through a safely opened parent directory."""
    with _directory(path.parent) as parent:
        descriptor = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent,
        )
        with os.fdopen(descriptor, encoding="utf-8") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError(f"Not a regular file: {path}")
            yield stream


@contextmanager
def _atomic_text(path: Path) -> Iterator[TextIO]:
    """Publish a complete report atomically, replacing only regular report files."""
    with _directory(path.parent, create=True) as parent:
        temporary = f".report-{uuid.uuid4().hex}"
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o664, dir_fd=parent,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                yield stream
                stream.flush()
                os.fsync(stream.fileno())
            _regular_at(parent, path.name)
            os.replace(temporary, path.name, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass


def _write_json(path: Path, value: Any) -> None:
    """Atomically serialize JSON metadata."""
    with _atomic_text(path) as stream:
        json.dump(value, stream, ensure_ascii=True, sort_keys=True)
        stream.write("\n")


def _read_json(path: Path) -> Any:
    """Load JSON metadata without following filesystem links."""
    with _reader(path) as stream:
        return json.load(stream)


def report_directory(output_root: str, log_name: str, run_id: str) -> Path:
    """Return the run-specific report path without creating it.

    :param str output_root: Absolute JPEG destination root.
    :param str log_name: Safe report label.
    :param str run_id: Airflow run identifier.
    :return: Run-specific report directory.
    :rtype: Path
    """
    root = _absolute(output_root)
    if not re.fullmatch(LOG_NAME_PATTERN, log_name):
        raise ValueError("log_name must contain 1–64 ASCII letters, digits, '_' or '-'")
    key = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return root / REPORT_DIRECTORY / log_name / key


def _load_run(report_path: Path) -> Run:
    """Load and validate the configuration for a run."""
    config = Run(**_read_json(report_path / "run.json"))
    expected = report_directory(config.output_root, config.log_name, config.run_id)
    if report_path != expected:
        raise ValueError("Report path does not match its run configuration")
    return config


def _probe_publication(root: Path) -> None:
    """Verify writable storage and atomic, non-replacing hard-link publication."""
    with _directory(root) as parent:
        temporary = f".mkderivs-probe-{uuid.uuid4().hex}"
        linked = f"{temporary}-link"
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o664, dir_fd=parent,
        )
        try:
            os.write(descriptor, b"mkderivs\n")
            os.fsync(descriptor)
            os.link(temporary, linked, src_dir_fd=parent, dst_dir_fd=parent)
            try:
                os.link(temporary, linked, src_dir_fd=parent, dst_dir_fd=parent)
            except FileExistsError:
                pass
            else:
                raise RuntimeError("Filesystem did not refuse replacement")
        finally:
            os.close(descriptor)
            for name in (temporary, linked):
                try:
                    os.unlink(name, dir_fd=parent)
                except FileNotFoundError:
                    pass


def initialize_run(input_root: str, output_root: str, log_name: str, run_id: str) -> str:
    """Validate storage and native codecs, then persist the run configuration.

    :param str input_root: Absolute directory containing the source tree.
    :param str output_root: Absolute directory for the mirrored JPEG tree.
    :param str log_name: Safe report label.
    :param str run_id: Airflow run identifier.
    :return: Absolute run report directory.
    :rtype: str
    """
    source, destination = _absolute(input_root), _absolute(output_root)
    if source.is_relative_to(destination) or destination.is_relative_to(source):
        raise ValueError("Input and output roots must not overlap")
    reports = report_directory(str(destination), log_name, run_id)
    with _directory(source) as parent:
        # Opening an iterator verifies read permission without retaining a listing.
        with os.scandir(parent) as entries:
            next(entries, None)
    with _directory(reports, create=True):
        pass
    _probe_publication(reports)
    vips = _vips()
    for operation in ("tiffload", "jpegload", "jpegsave_target"):
        if not vips.type_find("VipsOperation", operation):
            raise RuntimeError(f"Required native image operation is missing: {operation}")
    vips.Image.black(1, 1).jpegsave_buffer(
        Q=70, interlace=True, subsample_mode="on", strip=True,
    )
    config = Run(str(source), str(destination), log_name, run_id, {
        "pyvips": vips.__version__,
        "libvips": ".".join(str(vips.version(i)) for i in range(3)),
    })
    if (reports / "run.json").exists():
        previous = _load_run(reports)
        if previous != config:
            raise ValueError("Run configuration changed; trigger a new Dag run")
    else:
        _write_json(reports / "run.json", asdict(config))
    # Use the repository storage convention with an explicit shared-storage base.
    run_dir(reports.name, base_dir=str(reports.parent))
    logger.info("JPEG derivative reports: %s", reports)
    return str(reports)


def _walk(source: Path) -> Iterator[tuple[Path, os.stat_result]]:
    """Traverse deterministically without following symbolic links."""
    pending = [source]
    while pending:
        directory = pending.pop()
        children = []
        with _directory(directory) as descriptor:
            with os.scandir(descriptor) as entries:
                snapshots = sorted(
                    ((entry.name, entry.stat(follow_symlinks=False)) for entry in entries),
                    key=lambda entry: entry[0],
                )
        for name, info in snapshots:
            path = directory / name
            if stat.S_ISDIR(info.st_mode):
                children.append(path)
            else:
                yield path, info
        pending.extend(reversed(children))


def _catalog_inputs(config: Run, catalog: sqlite3.Connection, exclusions: TextIO) -> int:
    """Catalog inputs on disk, detecting output collisions before conversion."""
    catalog.execute(
        "CREATE TABLE files (relative TEXT PRIMARY KEY, output TEXT UNIQUE, "
        "size INTEGER, mtime_ns INTEGER)"
    )
    count = 0
    for path, info in _walk(Path(config.input_root)):
        relative = path.relative_to(config.input_root)
        reason = None
        if stat.S_ISLNK(info.st_mode):
            reason = "symlink"
        elif path.suffix.lower() not in {".tif", ".tiff"}:
            continue
        elif path.name.startswith("."):
            reason = "hidden filename"
        elif not stat.S_ISREG(info.st_mode):
            reason = "not a regular file"
        if reason:
            exclusions.write(json.dumps({"source": str(path), "reason": reason}) + "\n")
            logger.info("EXCLUDED %r: %s", str(path), reason)
            continue
        if relative.parts[0] == REPORT_DIRECTORY:
            raise ValueError(f"Input conflicts with reserved report namespace: {relative}")
        try:
            catalog.execute("INSERT INTO files VALUES (?, ?, ?, ?)", (
                str(relative), str(relative.with_suffix(".jpg")), info.st_size,
                info.st_mtime_ns,
            ))
        except sqlite3.IntegrityError as error:
            raise ValueError(f"Multiple inputs map to the same JPEG: {relative}") from error
        count += 1
    for (output,) in catalog.execute("SELECT output FROM files ORDER BY output"):
        for parent in Path(output).parents:
            if catalog.execute(
                "SELECT 1 FROM files WHERE output = ?", (str(parent),)
            ).fetchone():
                raise ValueError(f"Output file also required as a directory: {parent}")
    return count


def discover_inputs(report_path: str) -> list[dict[str, str]]:
    """Persist sorted, bounded batch manifests instead of large XCom payloads.

    :param str report_path: Validated run report directory.
    :return: Operator keyword arguments containing batch manifest paths.
    :rtype: list[dict[str, str]]
    """
    reports = Path(report_path)
    config = _load_run(reports)
    # A retry reuses the frozen discovery snapshot, including completed work.
    if (reports / "discovery.json").exists():
        return [{"manifest_path": path} for path in _read_json(
            reports / "discovery.json"
        )["batches"]]
    with tempfile.TemporaryDirectory(prefix="mkderivs-catalog-") as temporary:
        with closing(sqlite3.connect(str(Path(temporary) / "inputs.sqlite"))) as catalog:
            with _atomic_text(reports / "exclusions.jsonl") as exclusions:
                count = _catalog_inputs(config, catalog, exclusions)
            batch_size = max(TARGET_BATCH_SIZE, math.ceil(count / MAX_BATCHES))
            rows = iter(catalog.execute(
                "SELECT relative, size, mtime_ns FROM files ORDER BY relative"
            ))
            batches = []
            for index in range(math.ceil(count / batch_size)):
                manifest = reports / "batches" / f"{index:04}.jsonl"
                with _atomic_text(manifest) as stream:
                    for _ in range(batch_size):
                        row = next(rows, None)
                        if row is None:
                            break
                        stream.write(json.dumps(asdict(WorkItem(*row))) + "\n")
                batches.append(str(manifest))
    _write_json(reports / "discovery.json", {"count": count, "batches": batches})
    logger.info("Discovered %s TIFFs in %s batches", count, len(batches))
    return [{"manifest_path": path} for path in batches]


def _relative(value: str) -> Path:
    """Validate a relative manifest path before using it within either root."""
    path = Path(value)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise ValueError(f"Unsafe manifest path: {value!r}")
    if path.parts[0] == REPORT_DIRECTORY or path.suffix.lower() not in {".tif", ".tiff"}:
        raise ValueError(f"Unexpected source path: {value!r}")
    return path


class _NativeWarnings(logging.Handler):
    """Collect VIPS warnings and add source context before Airflow handles them."""

    def __init__(self, source: str, messages: list[str]) -> None:
        super().__init__(logging.WARNING)
        self.source = source
        self.messages = messages

    def emit(self, record: logging.LogRecord) -> None:
        """Keep the diagnostic and preserve propagation to the task logger.

        :param logging.LogRecord record: Native-library warning or error record.
        """
        message = record.getMessage()
        self.messages.append(message)
        record.msg = f"{self.source!r}: {message}"
        record.args = ()


def _check_image(image: Any) -> list[str]:
    """Reject image layouts requiring review, and describe metadata loss."""
    supported = {
        ("uchar", "b-w", 1), ("ushort", "grey16", 1),
        ("uchar", "srgb", 3), ("ushort", "rgb16", 3),
    }
    if image.hasalpha() or (image.format, image.interpretation, image.bands) not in supported:
        raise ImageReviewError(
            "Image requires review: "
            f"format={image.format}, interpretation={image.interpretation}, "
            f"bands={image.bands}, alpha={image.hasalpha()}"
        )
    messages = []
    if image.get_typeof("icc-profile-data"):
        messages.append("ICC profile stripped without converting pixels to sRGB")
    if image.get_typeof("orientation") and image.get("orientation") not in (0, 1):
        messages.append("Orientation metadata stripped; stored pixel orientation retained")
    return messages


def _encode(image: Any, descriptor: int) -> None:
    """Encode JPEG, preserving Python filesystem errors from the native writer."""
    vips = _vips()
    errors: list[OSError] = []
    target = vips.TargetCustom()

    def write(data: Any) -> int:
        try:
            return os.write(descriptor, data)
        except OSError as error:
            errors.append(error)
            return -1

    target.on_write(write)
    try:
        image.jpegsave_target(
            target, Q=70, interlace=True, subsample_mode="on", strip=True,
        )
    except vips.Error as error:
        if errors:
            raise errors[0] from error
        raise
    if errors:
        raise errors[0]
    os.fsync(descriptor)


def _matches(info: os.stat_result, item: WorkItem) -> bool:
    """Compare a current source identity with the frozen discovery snapshot."""
    return stat.S_ISREG(info.st_mode) and (
        info.st_size, info.st_mtime_ns
    ) == (item.size, item.mtime_ns)


@contextmanager
def _source_descriptor(config: Run, item: WorkItem) -> Iterator[int]:
    """Open a frozen source without following links and check its identity."""
    source = Path(config.input_root) / _relative(item.relative)
    try:
        with _directory(source.parent) as parent:
            try:
                descriptor = os.open(
                    source.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent,
                )
            except PermissionError as error:
                raise ImageReviewError("Source file is not readable") from error
    except OSError as error:
        if error.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
            raise ImageReviewError("Source disappeared or its directory changed") from error
        raise
    try:
        if not _matches(os.fstat(descriptor), item):
            raise ImageReviewError("Source changed since discovery")
        yield descriptor
    finally:
        os.close(descriptor)


def _publish_jpeg(
    image: Any, parent: int, filename: str, source_descriptor: int, item: WorkItem,
) -> bool:
    """Encode and check a staged image, then publish without replacing a destination."""
    temporary = f".mkderivs-{uuid.uuid4().hex}.jpg"
    descriptor = os.open(
        temporary, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o664, dir_fd=parent,
    )
    try:
        _encode(image, descriptor)
        encoded = _vips().Image.jpegload(f"/proc/self/fd/{descriptor}", fail_on="error")
        if (encoded.width, encoded.height) != (image.width, image.height):
            raise ImageReviewError("Encoded JPEG dimensions do not match")
        if not _matches(os.fstat(source_descriptor), item):
            raise ImageReviewError("Source changed during conversion")
        try:
            os.link(
                temporary, filename, src_dir_fd=parent, dst_dir_fd=parent,
                follow_symlinks=False,
            )
        except FileExistsError:
            _regular_at(parent, filename)
            return False
        os.fsync(parent)
        return True
    finally:
        os.close(descriptor)
        os.unlink(temporary, dir_fd=parent)


def _convert_item(config: Run, item: WorkItem) -> Result:
    """Convert and atomically publish one JPEG, distinguishing storage failures."""
    relative = _relative(item.relative)
    source = Path(config.input_root) / relative
    destination = Path(config.output_root) / relative.with_suffix(".jpg")
    result = Result(item.relative, str(source), str(destination), "ERROR")
    handler = _NativeWarnings(str(source), result.warnings)
    native_logger = logging.getLogger("pyvips")
    vips = _vips()
    native_logger.addHandler(handler)
    try:
        with _directory(destination.parent, create=True) as parent:
            if _regular_at(parent, destination.name):
                result.status = "SKIPPED"
                logger.info("SKIPPED %r -> %r", str(source), str(destination))
                return result
            logger.info("Converting %r -> %r", str(source), str(destination))
            with _source_descriptor(config, item) as source_descriptor:
                # Descriptor paths keep native reads pinned to the validated inode.
                image = vips.Image.tiffload(
                    f"/proc/self/fd/{source_descriptor}", page=0, n=1,
                    autorotate=False, access="sequential", fail_on="error",
                )
                result.warnings.extend(_check_image(image))
                result.input_size = [image.width, image.height]
                downscaled = max(image.width, image.height) > MAX_JPEG_DIMENSION
                if downscaled:
                    image = image.resize(
                        MAX_JPEG_DIMENSION / max(image.width, image.height),
                        kernel="lanczos3",
                    )
                if max(image.width, image.height) > MAX_JPEG_DIMENSION:
                    raise ImageReviewError("Resized image exceeds the JPEG dimension limit")
                result.output_size = [image.width, image.height]
                if _publish_jpeg(image, parent, destination.name, source_descriptor, item):
                    result.status = "DOWNSCALED" if downscaled else "CONVERTED"
                else:
                    result.status = "SKIPPED"
    except (ImageReviewError, vips.Error) as error:
        result.error = str(error)
        logger.warning("ERROR %r: %s", str(source), error)
    finally:
        native_logger.removeHandler(handler)
        handler.close()
    logger.info("%s %r -> %r", result.status, str(source), str(destination))
    return result


def _journal_results(path: Path) -> dict[str, Result]:
    """Recover complete checkpoint lines, ignoring only an interrupted final line."""
    results = {}
    try:
        with _reader(path) as stream:
            for line in stream:
                if not line.endswith("\n"):
                    break
                result = Result(**json.loads(line))
                results[result.relative] = result
    except FileNotFoundError:
        pass
    return results


def _reusable(result: Result) -> bool:
    """Require a still-present regular output before reusing a success checkpoint."""
    if result.status not in SUCCESS_STATUSES:
        return False
    try:
        with _directory(Path(result.output).parent) as parent:
            return _regular_at(parent, Path(result.output).name)
    except FileNotFoundError:
        return False


def _process_manifest(config: Run, manifest: Path) -> None:
    """Append synchronized per-file checkpoints and mark the completed batch."""
    journal = manifest.with_suffix(".results.jsonl")
    completed = manifest.with_suffix(".complete.json")
    with _directory(completed.parent) as parent:
        try:
            os.unlink(completed.name, dir_fd=parent)
        except FileNotFoundError:
            pass
    previous = _journal_results(journal)
    # Rewrite recovered lines atomically so a torn final line cannot join a retry's line.
    with _atomic_text(journal) as stream:
        for result in previous.values():
            stream.write(json.dumps(asdict(result)) + "\n")
    with _directory(journal.parent) as parent:
        descriptor = os.open(
            journal.name, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, dir_fd=parent,
        )
        with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
            with _reader(manifest) as inputs:
                count = 0
                for line in inputs:
                    item = WorkItem(**json.loads(line))
                    relative = _relative(item.relative)
                    checkpoint = previous.get(item.relative)
                    if checkpoint and checkpoint.output != str(
                        Path(config.output_root) / relative.with_suffix(".jpg")
                    ):
                        raise ValueError("Checkpoint output does not match its manifest")
                    if checkpoint and _reusable(checkpoint):
                        logger.info("Retained %s checkpoint: %r", checkpoint.status, item.relative)
                        count += 1
                        continue
                    result = _convert_item(config, item)
                    stream.write(json.dumps(asdict(result)) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                    count += 1
    _write_json(completed, {"count": count})


def convert_batch(manifest_path: str) -> None:
    """Convert a frozen batch, checkpointing every file and continuing image errors.

    :param str manifest_path: Absolute path to a run-owned batch manifest.
    """
    manifest = Path(manifest_path)
    reports = manifest.parent.parent
    config = _load_run(reports)
    discovery = _read_json(reports / "discovery.json")
    _batch_path(reports, manifest_path)
    if str(manifest) not in discovery["batches"]:
        raise ValueError("Batch manifest is not registered in this run")
    vips = _vips()
    old_concurrency = vips.vips_lib.vips_concurrency_get()
    old_cache = vips.cache_get_max()
    vips.vips_lib.vips_concurrency_set(2)
    vips.cache_set_max(0)
    try:
        _process_manifest(config, manifest)
    finally:
        vips.cache_set_max(old_cache)
        vips.vips_lib.vips_concurrency_set(old_concurrency)


def _batch_path(reports: Path, value: str) -> Path:
    """Restrict batch paths to the run's own manifest directory."""
    path = Path(value)
    if path.parent != reports / "batches" or not re.fullmatch(r"\d{4}\.jsonl", path.name):
        raise ValueError("Batch manifest is outside this run")
    return path


@dataclass
class _ReportWriters:
    """Streams and aggregate counts for a single report publication."""

    results: TextIO
    progress: TextIO
    errors: TextIO
    counts: Counter[str] = field(default_factory=Counter)

    def record(self, result: Result) -> None:
        """Write a per-input record and update aggregate counters."""
        self.counts[result.status.lower()] += 1
        self.counts["warnings"] += len(result.warnings)
        self.results.write(json.dumps(asdict(result)) + "\n")
        self.progress.write(f"{result.status} {result.source!r} -> {result.output!r}\n")
        for message in result.warnings:
            self.progress.write(f"  WARNING {message!r}\n")
        if result.error:
            self.errors.write(f"{result.source!r}\n{result.error}\n\n")
            self.progress.write(f"  ERROR {result.error!r}\n")


def _report_batch(manifest: Path, config: Run, writers: _ReportWriters) -> list[str]:
    """Merge a batch's latest records and describe incomplete work compactly."""
    results = _journal_results(manifest.with_suffix(".results.jsonl"))
    incomplete = []
    missing = expected = 0
    with _reader(manifest) as inputs:
        for line in inputs:
            item = WorkItem(**json.loads(line))
            relative = _relative(item.relative)
            expected += 1
            result = results.get(item.relative)
            if result is None:
                missing += 1
                result = Result(
                    item.relative, str(Path(config.input_root) / relative),
                    str(Path(config.output_root) / relative.with_suffix(".jpg")), "INCOMPLETE",
                )
            writers.record(result)
    if missing:
        incomplete.append(f"Missing results: {manifest.name} ({missing})")
    try:
        completion = _read_json(manifest.with_suffix(".complete.json"))
        if completion["count"] != expected:
            incomplete.append(f"Batch count mismatch: {manifest.name}")
    except FileNotFoundError:
        incomplete.append(f"Batch did not finish: {manifest.name}")
    return incomplete


def _report_exclusions(reports: Path, writers: _ReportWriters) -> list[str]:
    """Append discovery exclusions to progress output."""
    try:
        with _reader(reports / "exclusions.jsonl") as exclusions:
            for line in exclusions:
                exclusion = json.loads(line)
                writers.counts["excluded"] += 1
                writers.progress.write(
                    f"EXCLUDED {exclusion['source']!r}: {exclusion['reason']}\n"
                )
    except FileNotFoundError:
        return ["Missing discovery exclusions report"]
    return []


def _finish_errors(reports: Path, present: bool) -> None:
    """Publish errors only when present, removing an obsolete errors-only report."""
    with _directory(reports) as parent:
        previous = _regular_at(parent, "errors.txt")
        if present:
            os.replace(".errors.pending", "errors.txt", src_dir_fd=parent, dst_dir_fd=parent)
        else:
            os.unlink(".errors.pending", dir_fd=parent)
            if previous:
                os.unlink("errors.txt", dir_fd=parent)
        os.fsync(parent)


def finalize_reports(report_path: str) -> dict[str, Any]:
    """Consolidate portable reports and identify incomplete infrastructure work.

    :param str report_path: Validated run report directory.
    :return: Summary with counts, completion state, and report locations.
    :rtype: dict[str, Any]
    """
    reports = Path(report_path)
    config = _load_run(reports)
    incomplete = []
    try:
        discovery = _read_json(reports / "discovery.json")
    except FileNotFoundError:
        discovery = {"count": 0, "batches": []}
        incomplete.append("Discovery did not finish")
    with (
        _atomic_text(reports / "results.jsonl") as results_stream,
        _atomic_text(reports / "progress.txt") as progress,
        _atomic_text(reports / ".errors.pending") as errors,
    ):
        writers = _ReportWriters(results_stream, progress, errors)
        counts = writers.counts
        progress.write(f"Dag run: {config.run_id!r}\n")
        for value in discovery["batches"]:
            incomplete.extend(_report_batch(_batch_path(reports, value), config, writers))
        incomplete.extend(_report_exclusions(reports, writers))
        state = "complete"
        if not discovery["count"]:
            progress.write("WARNING No eligible TIFF files found\n")
            counts["warnings"] += 1
        if counts["error"] or counts["warnings"] or counts["excluded"]:
            state = "complete_with_warnings"
        if incomplete:
            state = "incomplete"
        progress.write(f"State: {state}\nCounts: {dict(counts)}\n")
    _finish_errors(reports, bool(counts["error"]))
    summary = {
        "run_id": config.run_id,
        "parameters": {
            "input_root": config.input_root, "output_root": config.output_root,
            "log_name": config.log_name,
        },
        "versions": config.versions,
        "state": state,
        "discovered": discovery["count"],
        "counts": {key: counts[key] for key in (
            "converted", "downscaled", "skipped", "error", "excluded", "warnings", "incomplete",
        )},
        "incomplete": incomplete,
        "reports": {name: str(reports / name) for name in (
            "progress.txt", "results.jsonl", "summary.json",
        )},
    }
    if counts["error"]:
        summary["reports"]["errors.txt"] = str(reports / "errors.txt")
    _write_json(reports / "summary.json", summary)
    logger.log(
        logging.WARNING if state != "complete" else logging.INFO,
        "JPEG derivatives %s: %s; reports: %s", state, summary["counts"], reports,
    )
    return summary
