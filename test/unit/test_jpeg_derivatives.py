"""Regression tests for TIFF conversion, safe publication, and portable reports."""

from __future__ import annotations

import errno
import json
import logging
import os
import struct
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import pyvips  # type: ignore[import-untyped]

from mokelumne.util import jpeg_derivatives as derivatives


@pytest.fixture(name="roots")
def image_roots(tmp_path: Path) -> tuple[Path, Path]:
    """Return disjoint input and output roots."""
    source, output = tmp_path / "input", tmp_path / "output"
    source.mkdir()
    return source, output


def make_tiff(path: Path, image: Any = None, **options: Any) -> None:
    """Write a real TIFF fixture."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if image is None:
        image = pyvips.Image.black(32, 16, bands=3).new_from_image([160, 90, 30])
        image = image.copy(interpretation="srgb")
    image.tiffsave(str(path), **options)


def start(roots: tuple[Path, Path]) -> tuple[Path, list[dict[str, str]]]:
    """Initialize and discover a test run."""
    reports = Path(derivatives.initialize_run(str(roots[0]), str(roots[1]), "test", "run/1"))
    return reports, derivatives.discover_inputs(str(reports))


def finish(reports: Path, batches: list[dict[str, str]]) -> dict:
    """Convert batches and gather the final summary."""
    for batch in batches:
        derivatives.convert_batch(**batch)
    return derivatives.finalize_reports(str(reports))


def results(reports: Path) -> list[dict]:
    """Read consolidated results."""
    return [json.loads(line) for line in (reports / "results.jsonl").read_text().splitlines()]


def jpeg_segments(path: Path) -> dict[int, bytes]:
    """Read JPEG header segments independently of libvips."""
    data = path.read_bytes()
    assert data[:2] == b"\xff\xd8"
    position = 2
    segments: dict[int, bytes] = {}
    while position < len(data):
        assert data[position] == 0xFF
        marker = data[position + 1]
        if marker in (0xDA, 0xD9):
            break
        length = struct.unpack_from(">H", data, position + 2)[0]
        payload = data[position + 4:position + 2 + length]
        segments[marker] = segments.get(marker, b"") + payload
        position += length + 2
    return segments


def test_nested_discovery_names_exclusions_and_progress(roots):
    """Mirror nested paths and record hidden-file and symlink exclusions."""
    source, output = roots
    names = [
        "root.TIF", "a/b/c/page.tiff", ".hidden-dir/visible.tif", 'a/$quote\'"[0].tif',
        "folder with spaces/図.tif",
    ]
    for name in names:
        make_tiff(source / name)
    make_tiff(source / ".hidden.tif")
    (source / "page.tif.bak").write_text("ignored")
    (source / "empty").mkdir()
    (source / "linked.tif").symlink_to(source / "root.TIF")
    (source / "linked-directory").symlink_to(source / "a", target_is_directory=True)
    (source / "broken-link").symlink_to(source / "absent")
    reports, batches = start(roots)
    summary = finish(reports, batches)
    assert summary["discovered"] == 5
    assert summary["counts"]["converted"] == 5
    assert summary["counts"]["excluded"] == 4
    assert summary["state"] == "complete_with_warnings"
    assert not (reports / "errors.txt").exists()
    assert not (output / "empty").exists()
    assert not (output / "linked-directory").exists()
    for name in names:
        assert (output / Path(name).with_suffix(".jpg")).is_file()
    assert [record["relative"] for record in results(reports)] == sorted(names)
    assert "EXCLUDED" in (reports / "progress.txt").read_text()
    assert "/" not in reports.name


@pytest.mark.parametrize("bad_name", ["", "../bad", "a/b", ".", "a\\b", "x" * 65])
def test_invalid_log_names(roots, bad_name):
    """Reject unsafe or empty report labels."""
    with pytest.raises(ValueError, match="log_name"):
        derivatives.initialize_run(str(roots[0]), str(roots[1]), bad_name, "run")


@pytest.mark.parametrize("relative", ["../outside", "relative"])
def test_relative_or_traversing_roots_rejected(roots, relative):
    """Reject relative and parent-traversing roots."""
    with pytest.raises(ValueError, match="absolute"):
        derivatives.initialize_run(relative, str(roots[1]), "test", "run")


def test_overlapping_roots_rejected(roots):
    """Reject equal or nested source and destination roots."""
    source, _ = roots
    for destination in (source, source / "child", source.parent):
        with pytest.raises(ValueError, match="overlap"):
            derivatives.initialize_run(str(source), str(destination), "test", "run")


def test_symlinked_root_and_report_components_rejected(roots):
    """Prevent writes through symlinked destination or report directories."""
    source, output = roots
    output.symlink_to(source.parent / "actual-output", target_is_directory=True)
    with pytest.raises(OSError):
        derivatives.initialize_run(str(source), str(output), "test", "run")
    output.unlink()
    output.mkdir()
    outside = source.parent / "outside"
    outside.mkdir()
    (output / ".mkderivs").symlink_to(outside, target_is_directory=True)
    with pytest.raises(OSError):
        derivatives.initialize_run(str(source), str(output), "test", "run")
    assert not list(outside.iterdir())


@pytest.mark.parametrize("names", [
    ["same.tif", "same.tiff"],
    ["parent.tif", "parent.jpg/child.tif", "parent.jpg-other.tif"],
    [".mkderivs/page.tif"],
])
def test_output_collisions_fail_before_conversion(roots, names):
    """Fail ambiguous output mappings before publishing any JPEG."""
    for name in names:
        make_tiff(roots[0] / name)
    with pytest.raises(ValueError):
        start(roots)
    assert not list(roots[1].rglob("*.jpg"))


def test_bounded_batches_and_frozen_discovery(roots, monkeypatch):
    """Keep mapping bounded and preserve the original discovery snapshot."""
    monkeypatch.setattr(derivatives, "MAX_BATCHES", 3)
    monkeypatch.setattr(derivatives, "TARGET_BATCH_SIZE", 2)
    for index in range(13):
        (roots[0] / f"{index:03}.tif").write_text("discovery only")
    reports, batches = start(roots)
    assert len(batches) == 3
    lengths = [len(Path(batch["manifest_path"]).read_text(encoding="utf-8").splitlines())
               for batch in batches]
    assert lengths == [5, 5, 3]
    make_tiff(roots[0] / "later.tif")
    assert derivatives.discover_inputs(str(reports)) == batches


def test_progressive_sampling_quality_and_metadata(roots):
    """Verify JPEG markers, quality tables, metadata stripping, and warnings."""
    image = pyvips.Image.black(48, 24, bands=3).new_from_image([160, 90, 30])
    image = image.copy(interpretation="srgb")
    image.set_type(pyvips.GValue.gint_type, "orientation", 6)
    image.set_type(pyvips.GValue.gstr_type, "image-description", "private test metadata")
    # Use a real built-in sRGB profile, without introducing a fixture dependency.
    profile_image = image.icc_transform("srgb")
    image.set_type(
        pyvips.GValue.blob_type, "icc-profile-data", profile_image.get("icc-profile-data"),
    )
    make_tiff(roots[0] / "page.tif", image)
    reports, batches = start(roots)
    finish(reports, batches)
    output = roots[1] / "page.jpg"
    segments = jpeg_segments(output)
    assert 0xC2 in segments  # SOF2: progressive, eight-bit JPEG.
    frame = segments[0xC2]
    assert frame[0] == 8
    assert struct.unpack_from(">HH", frame, 1) == (24, 48)
    assert frame[5] == 3
    assert [frame[7], frame[10], frame[13]] == [0x22, 0x11, 0x11]
    assert not any(marker in segments for marker in (0xE1, 0xE2, 0xED))
    # The standard quality-70 luminance DC quantizer is round(16 * 0.6) = 10.
    tables = segments[0xDB]
    assert any(tables[index] == 0 and tables[index + 1] == 10
               for index in range(0, len(tables), 65))
    record = results(reports)[0]
    assert len(record["warnings"]) == 2
    assert pyvips.Image.jpegload(str(output)).width == 48


@pytest.mark.parametrize("kind", ["gray8", "gray16", "rgb16", "bilevel"])
def test_supported_depths_preserve_midtones(roots, kind):
    """Convert supported sample types without clipping sixteen-bit midtones."""
    if kind == "rgb16":
        image = pyvips.Image.black(16, 16, bands=3).cast("ushort")
        image = image.new_from_image([32768] * 3).copy(interpretation="rgb16")
    elif kind == "gray16":
        image = pyvips.Image.black(16, 16).cast("ushort")
        image = image.new_from_image(32768).copy(interpretation="grey16")
    else:
        image = pyvips.Image.black(16, 16).new_from_image(128).copy(interpretation="b-w")
    options = {"bitdepth": 1} if kind == "bilevel" else {}
    make_tiff(roots[0] / "page.tif", image, **options)
    reports, batches = start(roots)
    summary = finish(reports, batches)
    assert summary["counts"]["converted"] == 1
    value = pyvips.Image.jpegload(str(roots[1] / "page.jpg")).avg()
    if kind != "bilevel":
        assert 120 < value < 135


@pytest.mark.parametrize("kind", ["alpha", "cmyk", "lab", "float"])
def test_unsupported_images_are_review_errors(roots, kind):
    """Retain review errors while allowing all-error batches to finish."""
    specifications = {
        "alpha": (4, "srgb", "uchar"), "cmyk": (4, "cmyk", "uchar"),
        "lab": (3, "lab", "float"), "float": (1, "b-w", "float"),
    }
    bands, interpretation, numeric = specifications[kind]
    image = pyvips.Image.black(16, 16, bands=bands).cast(numeric)
    make_tiff(roots[0] / "page.tif", image.copy(interpretation=interpretation))
    reports, batches = start(roots)
    summary = finish(reports, batches)
    assert summary["state"] == "complete_with_warnings"
    assert summary["counts"]["error"] == 1
    assert "requires review" in (reports / "errors.txt").read_text()
    assert not (roots[1] / "page.jpg").exists()


def test_only_first_page_is_converted(roots):
    """Select the first TIFF page rather than concatenating all pages."""
    image = pyvips.Image.black(32, 16).new_from_image(50)
    image = image.join(image.new_from_image(220), "vertical")
    make_tiff(roots[0] / "pages.tif", image, page_height=16)
    reports, batches = start(roots)
    finish(reports, batches)
    output = pyvips.Image.jpegload(str(roots[1] / "pages.jpg"))
    assert (output.width, output.height) == (32, 16)
    assert 45 < output.avg() < 55


@pytest.mark.parametrize("width,height,expected", [
    (65_500, 2, [65_500, 2]), (65_501, 2, [65_500, 2]),
    (2, 65_501, [2, 65_500]), (131_000, 1, [65_500, 1]),
])
def test_dimension_boundary_and_thin_images(roots, width, height, expected):
    """Enforce the legacy dimension limit without collapsing thin images."""
    make_tiff(roots[0] / "large.tif", pyvips.Image.black(width, height))
    reports, batches = start(roots)
    finish(reports, batches)
    record = results(reports)[0]
    assert record["output_size"] == expected
    assert record["status"] == ("CONVERTED" if width == 65_500 else "DOWNSCALED")


def test_existing_output_is_preserved_and_retry_keeps_original_results(roots):
    """Preserve existing output and reuse durable successful checkpoints."""
    make_tiff(roots[0] / "new.tif")
    make_tiff(roots[0] / "existing.tif")
    roots[1].mkdir()
    existing = roots[1] / "existing.jpg"
    existing.write_bytes(b"existing JPEG is deliberately not decoded")
    reports, batches = start(roots)
    summary = finish(reports, batches)
    before = (roots[1] / "new.jpg").stat()
    assert summary["counts"]["converted"] == summary["counts"]["skipped"] == 1
    with patch.object(derivatives, "_encode", side_effect=AssertionError("unexpected conversion")):
        again = finish(reports, batches)
    assert again == summary
    assert (roots[1] / "new.jpg").stat().st_ino == before.st_ino
    assert existing.read_bytes().startswith(b"existing JPEG")


@pytest.mark.parametrize("change", ["modify", "remove", "symlink", "fifo"])
def test_changed_sources_are_per_file_errors(roots, change):
    """Report changed, removed, and replaced sources without publishing JPEGs."""
    source = roots[0] / "page.tif"
    make_tiff(source)
    reports, batches = start(roots)
    if change == "modify":
        source.write_text("changed")
    else:
        source.unlink()
        if change == "symlink":
            target = roots[0].parent / "outside.tif"
            make_tiff(target)
            source.symlink_to(target)
        elif change == "fifo":
            os.mkfifo(source)
    summary = finish(reports, batches)
    assert summary["counts"]["error"] == 1
    assert not (roots[1] / "page.jpg").exists()


def test_corrupt_and_valid_files_complete_with_warnings(roots):
    """Continue after a bad TIFF and retain its diagnostics alongside successes."""
    (roots[0] / "bad.tif").write_bytes(b"not a TIFF")
    make_tiff(roots[0] / "good.tif")
    reports, batches = start(roots)
    summary = finish(reports, batches)
    assert summary["counts"]["error"] == summary["counts"]["converted"] == 1
    assert summary["state"] == "complete_with_warnings"
    assert "bad.tif" in (reports / "errors.txt").read_text()


def test_native_decoder_warning_is_captured_with_source_context(roots, caplog):
    """Capture a real libtiff warning in both task logs and portable results."""
    source = roots[0] / "warning.tif"
    # An invalid TIFF field type produces a real libtiff warning while retaining
    # an otherwise decodable image. Change XResolution's type in the little-endian IFD.
    make_tiff(source, pyvips.Image.black(16, 16))
    data = bytearray(source.read_bytes())
    assert data[:2] == b"II"
    offset = struct.unpack_from("<I", data, 4)[0]
    count = struct.unpack_from("<H", data, offset)[0]
    for index in range(count):
        entry = offset + 2 + index * 12
        if struct.unpack_from("<H", data, entry)[0] == 282:
            struct.pack_into("<H", data, entry + 2, 99)
            break
    else:
        pytest.fail("Fixture has no XResolution field")
    source.write_bytes(data)
    reports, batches = start(roots)
    with caplog.at_level(logging.WARNING, logger="pyvips"):
        finish(reports, batches)
    record = results(reports)[0]
    assert record["warnings"]
    assert any("warning.tif" in message.getMessage() for message in caplog.records)


def test_empty_input_has_portable_noop_summary(roots):
    """Finalize an empty map as a successful no-op with a warning."""
    reports, batches = start(roots)
    assert batches == []
    summary = finish(reports, batches)
    assert summary["discovered"] == 0
    assert summary["state"] == "complete_with_warnings"
    assert results(reports) == []
    assert not (reports / "errors.txt").exists()


def test_missing_results_are_reported_as_incomplete(roots):
    """Record unfinished work in the portable summary and progress output."""
    make_tiff(roots[0] / "page.tif")
    reports, _ = start(roots)
    summary = derivatives.finalize_reports(str(reports))
    assert summary["state"] == "incomplete"
    assert any("Missing result" in error for error in summary["incomplete"])
    assert "INCOMPLETE" in (reports / "progress.txt").read_text()


def test_publication_race_never_overwrites_existing_file(roots, monkeypatch):
    """Preserve a destination published concurrently with conversion."""
    make_tiff(roots[0] / "page.tif")
    reports, batches = start(roots)
    real_link = os.link

    def race(source, destination, **kwargs):
        if destination == "page.jpg":
            (roots[1] / "page.jpg").write_bytes(b"other publisher")
        return real_link(source, destination, **kwargs)

    monkeypatch.setattr(os, "link", race)
    summary = finish(reports, batches)
    assert summary["counts"]["skipped"] == 1
    assert (roots[1] / "page.jpg").read_bytes() == b"other publisher"
    assert not list(roots[1].glob(".mkderivs-*.jpg"))


def test_destination_symlink_is_rejected(roots):
    """Reject a destination symlink without touching its target."""
    make_tiff(roots[0] / "page.tif")
    reports, batches = start(roots)
    outside = roots[0].parent / "outside.jpg"
    outside.write_bytes(b"preserve")
    (roots[1] / "page.jpg").symlink_to(outside)
    with pytest.raises(ValueError, match="Unsafe"):
        derivatives.convert_batch(**batches[0])
    assert outside.read_bytes() == b"preserve"
    assert derivatives.finalize_reports(str(reports))["state"] == "incomplete"


def test_native_output_storage_error_is_fatal_and_cleans_staging(roots, monkeypatch):
    """Propagate native-writer disk failures and remove incomplete staging files."""
    make_tiff(roots[0] / "page.tif")
    reports, batches = start(roots)
    real_write = os.write

    def full(descriptor, data):
        if os.readlink(f"/proc/self/fd/{descriptor}").endswith(".jpg"):
            raise OSError(errno.ENOSPC, "disk full")
        return real_write(descriptor, data)

    monkeypatch.setattr(os, "write", full)
    with pytest.raises(OSError, match="disk full"):
        derivatives.convert_batch(**batches[0])
    assert not (roots[1] / "page.jpg").exists()
    assert not list(roots[1].glob(".mkderivs-*.jpg"))
    assert derivatives.finalize_reports(str(reports))["state"] == "incomplete"


def test_interrupted_batch_reuses_checkpoints_and_recovers_torn_tail(roots, monkeypatch):
    """Recover a partially processed batch and a torn checkpoint line."""
    make_tiff(roots[0] / "a.tif")
    make_tiff(roots[0] / "b.tif")
    reports, batches = start(roots)
    real_encode = derivatives._encode  # pylint: disable=protected-access
    calls = 0

    def interrupt(image, descriptor):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError(errno.EIO, "interrupted")
        real_encode(image, descriptor)

    with monkeypatch.context() as context:
        context.setattr(derivatives, "_encode", interrupt)
        with pytest.raises(OSError, match="interrupted"):
            derivatives.convert_batch(**batches[0])
    first = (roots[1] / "a.jpg").stat()
    journal = Path(batches[0]["manifest_path"]).with_suffix(".results.jsonl")
    with journal.open("a", encoding="utf-8") as stream:
        stream.write('{"interrupted":')
    summary = finish(reports, batches)
    assert summary["counts"]["converted"] == 2
    assert (roots[1] / "a.jpg").stat().st_ino == first.st_ino
    assert len(results(reports)) == 2


def test_manifest_traversal_is_rejected(roots):
    """Reject an altered manifest attempting to escape the source root."""
    make_tiff(roots[0] / "page.tif")
    _, batches = start(roots)
    manifest = Path(batches[0]["manifest_path"])
    item = json.loads(manifest.read_text(encoding="utf-8"))
    item["relative"] = "../outside.tif"
    manifest.write_text(json.dumps(item) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Unsafe manifest"):
        derivatives.convert_batch(**batches[0])


@pytest.mark.parametrize("options", [
    {"compression": "lzw"}, {"compression": "deflate"}, {"compression": "packbits"},
    {"compression": "jpeg"}, {"tile": True, "compression": "lzw"}, {"bigtiff": True},
])
def test_collection_tiff_encodings(roots, options):
    """Read common compressed, tiled, and BigTIFF inputs through the native loader."""
    make_tiff(roots[0] / "page.tif", **options)
    reports, batches = start(roots)
    summary = finish(reports, batches)
    assert summary["counts"]["converted"] == 1
    assert summary["counts"]["error"] == 0


def test_corrupt_strip_fails_during_encoding_without_publishing(roots):
    """Catch lazy TIFF pixel failures and clean up partially written JPEGs."""
    source = roots[0] / "page.tif"
    make_tiff(source, pyvips.Image.black(32, 32))
    data = bytearray(source.read_bytes())
    offset = struct.unpack_from("<I", data, 4)[0]
    count = struct.unpack_from("<H", data, offset)[0]
    for index in range(count):
        entry = offset + 2 + index * 12
        if struct.unpack_from("<H", data, entry)[0] == 273:
            struct.pack_into("<I", data, entry + 8, len(data) + 1000)
            break
    else:
        pytest.fail("Fixture has no StripOffsets tag")
    source.write_bytes(data)
    reports, batches = start(roots)
    with patch.object(
        derivatives, "_encode", wraps=derivatives._encode,  # pylint: disable=protected-access
    ) as encode:
        summary = finish(reports, batches)
    assert encode.called
    assert summary["counts"]["error"] == 1
    assert not (roots[1] / "page.jpg").exists()
    assert not list(roots[1].glob(".mkderivs-*.jpg"))


def test_replaced_destination_parent_cannot_escape_output_root(roots):
    """Reject a destination directory replaced with an external symlink."""
    make_tiff(roots[0] / "folder/page.tif")
    _, batches = start(roots)
    outside = roots[0].parent / "outside"
    outside.mkdir()
    (roots[1] / "folder").symlink_to(outside, target_is_directory=True)
    with pytest.raises(OSError):
        derivatives.convert_batch(**batches[0])
    assert not list(outside.iterdir())


def test_checkpoint_cannot_redirect_retry_outside_the_output_root(roots):
    """Reject a modified checkpoint before it can redirect retry behavior."""
    make_tiff(roots[0] / "page.tif")
    reports, batches = start(roots)
    finish(reports, batches)
    journal = Path(batches[0]["manifest_path"]).with_suffix(".results.jsonl")
    record = json.loads(journal.read_text(encoding="utf-8"))
    record["output"] = str(roots[0].parent / "outside.jpg")
    journal.write_text(json.dumps(record) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Checkpoint output"):
        derivatives.convert_batch(**batches[0])


def test_report_publication_failure_propagates(roots, monkeypatch):
    """Fail finalization when report publication encounters a filesystem error."""
    make_tiff(roots[0] / "page.tif")
    reports, batches = start(roots)
    for batch in batches:
        derivatives.convert_batch(**batch)
    real_replace = os.replace

    def fail_summary(source, destination, **kwargs):
        if destination == "summary.json":
            raise OSError(errno.EIO, "report storage failed")
        return real_replace(source, destination, **kwargs)

    monkeypatch.setattr(os, "replace", fail_summary)
    with pytest.raises(OSError, match="report storage failed"):
        derivatives.finalize_reports(str(reports))
    assert not (reports / "summary.json").exists()


def test_hard_link_probe_does_not_fall_back_to_replacement(roots):
    """Fail validation when the shared filesystem does not support hard links."""
    with patch.object(os, "link", side_effect=OSError(errno.ENOTSUP, "hard links unsupported")):
        with pytest.raises(OSError, match="hard links unsupported"):
            start(roots)
    assert not list(roots[1].rglob(".mkderivs-probe-*"))


def test_existing_outputs_skip_unreadable_sources(roots):
    """Skip an existing JPEG before attempting to read its TIFF source."""
    source = roots[0] / "page.tif"
    make_tiff(source)
    reports, batches = start(roots)
    (roots[1] / "page.jpg").write_bytes(b"existing output")
    source.chmod(0)
    summary = finish(reports, batches)
    assert summary["counts"]["skipped"] == 1
    assert summary["counts"]["error"] == 0


def test_unreadable_source_is_a_per_file_error(roots):
    """Continue with other TIFFs after a source-file permission failure."""
    source = roots[0] / "unreadable.tif"
    make_tiff(source)
    make_tiff(roots[0] / "good.tif")
    reports, batches = start(roots)
    source.chmod(0)
    summary = finish(reports, batches)
    assert summary["counts"]["error"] == 1
    assert summary["counts"]["converted"] == 1
