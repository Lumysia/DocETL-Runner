"""Convert pipeline JSON output to multi-sheet Excel workbooks."""

import json
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from docetl_runner.constants import (
    DEFAULT_PRIMARY_METADATA_COLUMNS,
    EXCEL_CELL_MAX_LENGTH,
    EXCEL_ENGINE,
    EXCEL_EXCLUDED_COLUMNS,
    EXCEL_ILLEGAL_CONTROL_CHAR_PATTERN,
    EXCEL_INLINE_TAG_PATTERNS,
    EXCEL_PROGRESS_DESCRIPTION,
    EXCEL_SHEET_NAME_MAX_LENGTH,
    EXCEL_TRUNCATION_SUFFIX,
    EXCEL_XML_COMMENT_PATTERN,
    FILE_ENCODING,
    NULL_STRING,
)

logger = logging.getLogger(__name__)


def _clean_value(value: Any) -> Any:
    """Replace the sentinel null string with ``None``."""
    if value == NULL_STRING:
        return None
    return value


def _sanitize_for_excel(value: Any) -> Any:
    """Sanitize a value to be safe for Excel export.

    Removes or replaces illegal characters that openpyxl cannot handle,
    such as control characters and certain HTML/XML markers.

    Args:
        value: The value to sanitize.

    Returns:
        A sanitized version of the value safe for Excel export.
    """
    if value is None:
        return None

    if not isinstance(value, str):
        return value

    cleaned = re.sub(EXCEL_ILLEGAL_CONTROL_CHAR_PATTERN, "", value)
    cleaned = re.sub(EXCEL_XML_COMMENT_PATTERN, "", cleaned, flags=re.DOTALL)
    for pattern in EXCEL_INLINE_TAG_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned)
    if len(cleaned) > EXCEL_CELL_MAX_LENGTH:
        cleaned = cleaned[:EXCEL_CELL_MAX_LENGTH] + EXCEL_TRUNCATION_SUFFIX

    return cleaned


def _parse_nested_items(raw: Any) -> list[dict[str, Any]]:
    """Return list items when *raw* contains a list of dictionaries."""
    if raw is None:
        return []

    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped or stripped == NULL_STRING:
            return []
        try:
            raw = json.loads(stripped)
        except json.JSONDecodeError:
            return []

    if not isinstance(raw, list):
        return []

    return [item for item in raw if isinstance(item, dict)]


def _extract_scalar_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Extract top-level scalar metadata fields from a record."""
    metadata: dict[str, Any] = {}
    for key, value in record.items():
        if key in EXCEL_EXCLUDED_COLUMNS:
            continue
        if _parse_nested_items(value):
            continue
        if isinstance(value, (dict, list)):
            continue
        metadata[key] = _sanitize_for_excel(_clean_value(value))
    return metadata


def _extract_nested_rows(
    record: dict[str, Any],
    column: str,
) -> list[dict[str, Any]]:
    """Flatten nested data from *column* in a single *record*."""
    metadata = _extract_scalar_metadata(record)
    raw_items = _parse_nested_items(record.get(column))

    rows: list[dict[str, Any]] = []
    for item in raw_items:
        row = metadata.copy()
        for key, val in item.items():
            row[key] = _sanitize_for_excel(_clean_value(val))
        rows.append(row)
    return rows


def _discover_nested_columns(records: list[dict[str, Any]]) -> list[str]:
    """Discover top-level fields that contain list-of-dict structures."""
    nested_columns: set[str] = set()
    for record in records:
        for key, value in record.items():
            if isinstance(value, list):
                nested_columns.add(key)
    return sorted(nested_columns)


def _sort_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder DataFrame columns with primary metadata first."""
    prioritized = [
        column for column in DEFAULT_PRIMARY_METADATA_COLUMNS if column in df.columns
    ]
    other_cols = sorted(column for column in df.columns if column not in prioritized)
    ordered = prioritized + other_cols
    return df.reindex(columns=ordered)


def convert_json_to_excel(input_file: Path, output_file: Path) -> None:
    """Convert pipeline output JSON into a multi-sheet Excel workbook.

    Each top-level list-of-dict field becomes its own sheet.

    Args:
        input_file: Path to the JSON results file.
        output_file: Destination ``.xlsx`` path.

    Raises:
        FileNotFoundError: *input_file* does not exist.
        ValueError: JSON content is not a list.
        RuntimeError: Excel write failure.
    """
    with open(input_file, encoding=FILE_ENCODING) as fh:
        records = json.load(fh)

    if not isinstance(records, list):
        raise ValueError("Input JSON must be a list of records.")

    records = [record for record in records if isinstance(record, dict)]
    nested_columns = _discover_nested_columns(records)
    if not nested_columns:
        raise ValueError(
            "Input JSON does not contain any top-level list-of-dict fields to export."
        )

    sheets: dict[str, list[dict[str, Any]]] = {col: [] for col in nested_columns}

    with Progress(
        TextColumn(EXCEL_PROGRESS_DESCRIPTION),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    ) as progress:
        task = progress.add_task("records", total=len(records))
        for record in records:
            for col_name in nested_columns:
                items = _extract_nested_rows(record, col_name)
                if items:
                    sheets[col_name].extend(items)
            progress.advance(task)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with pd.ExcelWriter(output_file, engine=EXCEL_ENGINE) as writer:
            sheets_written = 0
            for sheet_name, data in sheets.items():
                df = _sort_columns(pd.DataFrame(data if data else []))
                # Sanitize all string values in the DataFrame
                for col in df.columns:
                    if df[col].dtype == "object":
                        df[col] = df[col].apply(_sanitize_for_excel)
                safe_name = sheet_name[:EXCEL_SHEET_NAME_MAX_LENGTH]
                df.to_excel(writer, sheet_name=safe_name, index=False)
                sheets_written += 1
                logger.info("Sheet '%s': %d row(s)", safe_name, len(df))

            if sheets_written == 0:
                logger.warning("No sheet data was written to the Excel workbook")
    except Exception as exc:
        raise RuntimeError(f"Failed to write Excel file: {exc}") from exc

    logger.info("Excel saved to %s", output_file)
