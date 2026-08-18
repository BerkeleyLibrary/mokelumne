from pathlib import Path
from collections.abc import Iterable

from pymarc.marcxml import map_xml
from yarl import URL

def extract_url_names(url_list: Iterable[str]) -> list[str]:
    """Converts a list of URLs into a list of their file basenames."""
    return [URL(url).name for url in url_list if url]


def list_values_from_marc_xml(
    xml_file_path: str | Path,
    field_num: str,
    ind1: str,
    ind2: str,
    sub: str,
) -> list[str]:
    """Parses a MARCXML file and returns a flat list of matching field values."""
    xml_path = Path(xml_file_path)

    if not xml_path.exists():
        raise FileNotFoundError(f"MARCXML file does not exist: {xml_path}")

    matching_values: list[str] = []

    def process_record(record) -> None:
        fields = record.get_fields(field_num)

        matching_values.extend(
            subfield
            for field in fields
            if field.indicator1 == ind1
            and field.indicator2 == ind2
            and (subfield := field.get(sub))
        )

    map_xml(process_record, str(xml_path))

    return matching_values
