from pathlib import Path

def validate_source_path(source_path: Path) -> None:
    """Validate the source directory containing document subdirectories."""

    if not source_path.exists():
        raise FileNotFoundError(
            f"Source directory does not exist: {source_path}"
        )

    if not source_path.is_dir():
        raise ValueError(
            f"Source path is not a directory: {source_path}"
        )

def validate_destination_path(destination_path: Path) -> None:
    """Validate the destination directory exists."""

    if not destination_path.exists():
        raise FileNotFoundError(
            f"Destination directory does not exist: {destination_path}"
        )

    if not destination_path.is_dir():
        raise ValueError(
            f"Destination path is not a directory: {destination_path}"
        )


# def validate_source_structure(...):
#     ...

