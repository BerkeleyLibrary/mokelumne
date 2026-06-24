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


def build_manifest(source_path: Path) -> list[dict[str, int | str]]:
    manifest = []

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


def write_manifest(manifest: list[dict[str, int | str]], manifest_path: Path) -> None:
    with open(manifest_path, "w", encoding="utf-8") as manifest_file:
        json.dump(manifest, manifest_file, indent=2)


def read_manifest(manifest_path: Path) -> list[dict[str, int | str]]:
    with open(manifest_path, "r", encoding="utf-8") as manifest_file:
        return json.load(manifest_file)


def verify_manifest(destination_path: Path, manifest_path: Path) -> None:
    """Verify all copied files exist at the destination."""

    manifest = read_manifest(manifest_path)

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


def copy_manifest_files(source_path: Path, destination_path: Path, manifest_path: Path) -> None:
    """Copy all files in manifest to destination directory"""

    manifest = read_manifest(manifest_path)

    for entry in manifest:
        relative_path = Path(str(entry["path"]))
        source_file = source_path / relative_path
        destination_file = destination_path / relative_path

        if not source_file.exists():
            raise FileNotFoundError(f"Manifest file missing from source: {source_file}")

        if not source_file.is_file():
            raise ValueError(f"Manifest path is not a file: {source_file}")
        

        destination_file.parent.mkdir(parents=True, exist_ok=True)
            
        # logger.info("Copying %s to %s", source_file,  destination_file)
        shutil.copy2(source_file, destination_file)

