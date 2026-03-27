"""PDF discovery and input JSON generation."""

import json
import logging
from pathlib import Path

from docetl_runner.constants import (
    FILE_ENCODING,
    INPUT_FIELD_FILENAME,
    INPUT_FIELD_PDF_PATH,
    PDF_GLOB_PATTERN,
)

logger = logging.getLogger(__name__)


def validate_input_folder(folder_path: Path) -> None:
    """Validate that *folder_path* exists and is a directory.

    Raises:
        FileNotFoundError: Folder does not exist.
        NotADirectoryError: Path is not a directory.
    """
    if not folder_path.exists():
        raise FileNotFoundError(f"Input folder does not exist: {folder_path}")
    if not folder_path.is_dir():
        raise NotADirectoryError(f"Input path is not a directory: {folder_path}")


def discover_pdf_files(folder_path: Path) -> list[Path]:
    """Return a sorted list of PDF files found in *folder_path*.

    Raises:
        ValueError: No PDF files found.
    """
    pdf_files = sorted(folder_path.glob(PDF_GLOB_PATTERN))
    if not pdf_files:
        raise ValueError(f"No PDF files found in: {folder_path}")
    logger.info("Discovered %d PDF file(s) in %s", len(pdf_files), folder_path)
    return pdf_files


def create_input_json(pdf_files: list[Path], output_path: Path) -> None:
    """Write a JSON manifest listing each PDF with its absolute path.

    Args:
        pdf_files: PDF paths to include.
        output_path: Destination for the JSON file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            INPUT_FIELD_FILENAME: pdf.name,
            INPUT_FIELD_PDF_PATH: str(pdf.absolute()),
        }
        for pdf in pdf_files
    ]
    with open(output_path, "w", encoding=FILE_ENCODING) as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
    logger.info("Created input JSON: %s", output_path)
