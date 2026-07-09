"""Provides file transfer routines."""

import hashlib
import json
import re
import shutil

from pathlib import Path

ManifestEntry = dict[str, int | str]
Manifest = dict[str, str | list[ManifestEntry]]

SOURCE_VOLUMES = [
    "/srv/lpsdata4",
    "/srv/dcushare",
    "/srv/lpsdata2",
    "/srv/ealdata",
    "/srv/rohoshare",
]

DESTINATION_VOLUMES = [
    "/srv/da",
    "/srv/pa",
]


def build_volume_path(
    volume: str,
    subdirectory: str,
    label: str,
) -> Path:
    """Build a path from a volume and relative subdirectory."""

    if subdirectory == "":
        raise ValueError(f"{label} subdirectory is required")

    subdirectory_path = Path(subdirectory)

    if subdirectory_path.is_absolute():
        raise ValueError(f"{label} subdirectory must be relative: {subdirectory}")

    return Path(volume) / subdirectory_path


def sha256_for_file(path: Path) -> str:
    """Calculate the SHA256 checksum for a file."""
    sha256 = hashlib.sha256()

    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def build_file_manifest(
        source_path: Path,
        exclude_regex: str | None = None,
    ) -> Manifest:
    exregex = re.compile(exclude_regex or r"^(\.(.*)|(?i:Thumbs\.db))$")

    files = [
        { "path": str(f.relative_to(source_path)), "size": f.stat().st_size, "sha256": sha256_for_file(f) }
        for f in source_path.rglob("*")
        if (
            f.is_file()
            and not any(re.search(exregex, p) for p in f.relative_to(source_path).parts)
        )
    ]
    
    return {
        "source_root": str(source_path),
        "files": files,
    }


def save_json(data: Manifest | list[ManifestEntry], path: Path) -> None:
    with open(path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=2)


def load_json(path: Path) -> Manifest:
    with open(path, "r", encoding="utf-8") as json_file:
        return json.load(json_file)


def clean_destination_path(destination_path: Path) -> Path:
    """Normalize a destination path to end with /incoming."""
    if destination_path.name.casefold() == "incoming":
        destination_path = destination_path.parent

    return Path(destination_path / "incoming")


def verify_file_manifest(destination_path: Path, manifest_path: Path) -> list[ManifestEntry]:
    """Verify all copied files exist at the destination."""

    verification_report: list[ManifestEntry] = []
    manifest = load_json(manifest_path)

    for entry in manifest["files"]:
        relative_path = Path(str(entry.get("path")))
        expected_size = entry["size"]
        expected_sha256 = entry["sha256"]

        destination_file = destination_path / relative_path

        if not destination_file.exists():
            raise FileNotFoundError(f"File not found: {destination_file}")

        if not destination_file.is_file():
            raise ValueError(f"Path exists but is not a file: {destination_file}")

        actual_size = destination_file.stat().st_size

        if actual_size != expected_size:
            raise ValueError(
                f"Size mismatch for {destination_file}: "
                f"expected {expected_size}, got {actual_size}"
            )

        actual_sha256 = sha256_for_file(destination_file)

        if actual_sha256 != expected_sha256:
            raise ValueError(
                f"Checksum mismatch for {destination_file}: "
                f"expected {expected_sha256}, got {actual_sha256}"
            )

        verification_report.append(
            {
                "path": str(relative_path),
                "status": "verified",
                "size": expected_size,
                "sha256": expected_sha256,
            }
        )

    return verification_report

def rename_temp_dir(temp_dir: Path):
    """When copy is complete will rename tmp dir to incoming"""

    base_dir = temp_dir.parent.parent

    incoming_dir = base_dir / "incoming" 
    temp_dir.rename(incoming_dir)


def copy_files_from_manifest(source_path: Path, destination_path: Path, manifest_path: Path) -> None:
    """Copy all files in manifest to destination directory."""

    manifest = load_json(manifest_path)

    for entry in manifest["files"]:
        relative_path = Path(str(entry["path"]))
        source_file = source_path / relative_path
        destination_file = destination_path / relative_path

        if not source_file.exists():
            raise FileNotFoundError(f"Manifest file missing from source: {source_file}")

        if not source_file.is_file():
            raise ValueError(f"Manifest path is not a file: {source_file}")

        destination_file.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(source_file, destination_file)
