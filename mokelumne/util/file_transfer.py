"""Provides file transfer routines."""

import hashlib
import json
import shutil

from pathlib import Path


def sha256_for_file(path: Path) -> str:
    """Calculate the SHA256 checksum for a file."""
    sha256 = hashlib.sha256()

    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            sha256.update(chunk)
    
    return sha256.hexdigest()


def build_file_manifest(source_path: Path) -> list[dict[str, int | str]]:
    manifest = []

    # TODO: Decide if we want to change relative_path to absolute_path
    # or save the root path once
    for item in source_path.rglob("*"):
        if item.is_file():
            relative_path = item.relative_to(source_path)
            manifest.append(
                {
                    "path": str(relative_path),
                    "size": item.stat().st_size,
                    "sha256": sha256_for_file(item),
                }
            )
    
    return manifest


def save_json(data: list[dict[str, int | str]], path: Path) -> None:
    with open(path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=2)


def load_json(path: Path) -> list[dict[str, int | str]]:
    with open(path, "r", encoding="utf-8") as json_file:
        return json.load(json_file)


def verify_file_manifest(destination_path: Path, manifest_path: Path) -> list[dict[str, int | str]]:
    """Verify all copied files exist at the destination."""

    verification_report: list[dict[str, int | str]] = []
    manifest = load_json(manifest_path)

    for entry in manifest:
        relative_path = Path(str(entry["path"]))
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


def copy_files_from_manifest(source_path: Path, destination_path: Path, manifest_path: Path) -> None:
    """Copy all files in manifest to destination directory."""

    manifest = load_json(manifest_path)

    for entry in manifest:
        relative_path = Path(str(entry["path"]))
        source_file = source_path / relative_path
        destination_file = destination_path / relative_path

        if not source_file.exists():
            raise FileNotFoundError(f"Manifest file missing from source: {source_file}")

        if not source_file.is_file():
            raise ValueError(f"Manifest path is not a file: {source_file}")
        
        destination_file.parent.mkdir(parents=True, exist_ok=True)
            
        shutil.copy2(source_file, destination_file)
