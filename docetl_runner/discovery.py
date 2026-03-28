"""PDF discovery and input JSON generation."""

import json
import logging
from pathlib import Path

from docetl_runner.constants import (
    BATCH_FILE_INFIX,
    FILE_ENCODING,
    INPUT_FIELD_FILENAME,
    INPUT_FIELD_PDF_PATH,
    PDF_GLOB_PATTERN,
)
from docetl_runner.docling import stage_pdf_path_for_pipeline

logger = logging.getLogger(__name__)


def _build_manifest_records(
    pdf_files: list[Path], *, staging_root: Path | None = None
) -> list[dict[str, str]]:
    """Build manifest records with filesystem-neutral, JSON-safe paths."""
    return [
        {
            INPUT_FIELD_FILENAME: pdf.name,
            INPUT_FIELD_PDF_PATH: str(
                stage_pdf_path_for_pipeline(pdf.resolve(), staging_root)
                if staging_root is not None
                else pdf.resolve()
            ),
        }
        for pdf in pdf_files
    ]


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
    records = _build_manifest_records(pdf_files, staging_root=output_path.parent)
    with open(output_path, "w", encoding=FILE_ENCODING) as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
    logger.info("Created input JSON: %s", output_path)


def create_batched_input_json(
    pdf_files: list[Path],
    output_dir: Path,
    folder_name: str,
    batch_size: int,
) -> list[Path]:
    """Split PDFs into multiple batch JSON files.

    Args:
        pdf_files: PDF paths to split into batches.
        output_dir: Directory for batch JSON files.
        folder_name: Name of the input folder (used for naming batches).
        batch_size: Number of PDFs per batch.

    Returns:
        List of paths to created batch JSON files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_files = []
    total_batches = (len(pdf_files) + batch_size - 1) // batch_size

    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(pdf_files))
        batch_pdfs = pdf_files[start_idx:end_idx]

        batch_filename = f"{folder_name}{BATCH_FILE_INFIX}{batch_num + 1}.json"
        batch_path = output_dir / batch_filename

        records = _build_manifest_records(batch_pdfs, staging_root=output_dir)

        with open(batch_path, "w", encoding=FILE_ENCODING) as fh:
            json.dump(records, fh, ensure_ascii=False, indent=2)

        batch_files.append(batch_path)
        logger.info(
            "Created batch %d/%d: %s (%d PDFs)",
            batch_num + 1,
            total_batches,
            batch_filename,
            len(batch_pdfs),
        )

    logger.info("Created %d batch file(s) for %d PDFs", total_batches, len(pdf_files))
    return batch_files
