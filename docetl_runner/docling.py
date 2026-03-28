"""Shared Docling helpers for DocETL pipeline parsing tools."""

from __future__ import annotations

import gc
import hashlib
import logging
import os
import shutil
import tempfile
import threading
import unicodedata
from pathlib import Path

import huggingface_hub.file_download as hf_file_download
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    AcceleratorDevice,
    AcceleratorOptions,
    PdfPipelineOptions,
)
from docling.document_converter import DocumentConverter, PdfFormatOption

from docetl_runner.constants import (
    DOCLING_DEFAULT_INPUT_KEY,
    DOCLING_DEFAULT_INTERMEDIATE_ROOT,
    DOCLING_DEFAULT_OUTPUT_KEY,
    DOCLING_DEFAULT_THREADS,
    DOCLING_INTERMEDIATE_PREFIX,
    DOCLING_PDF_DEFAULT_FILENAME,
    DOCLING_SAFE_FILENAME_ALLOWED_CHARS,
    DOCLING_STAGING_DIR_NAME,
    DOCLING_THREADS_ENV_VAR,
    DOCLING_WINDOWS_SYMLINK_PERMISSION_WINERROR,
    INPUT_FIELD_FILENAME,
)

LOGGER = logging.getLogger("docetl_runner.docling")
_original_symlink = getattr(hf_file_download.os, "symlink", None)
_converter: DocumentConverter | None = None
_converter_init_lock = threading.Lock()
_converter_use_lock = threading.Lock()


def _install_windows_safe_symlink() -> None:
    if os.name != "nt" or _original_symlink is None:
        return

    def _safe_symlink(src: str, dst: str, *args: object, **kwargs: object) -> None:
        try:
            _original_symlink(src, dst, *args, **kwargs)
        except OSError as exc:
            if (
                getattr(exc, "winerror", None)
                == DOCLING_WINDOWS_SYMLINK_PERMISSION_WINERROR
            ):
                raise PermissionError(
                    "Symlink privilege not available on Windows"
                ) from exc
            raise

    hf_file_download.os.symlink = _safe_symlink


_install_windows_safe_symlink()


def _build_converter() -> DocumentConverter:
    num_threads = get_docling_num_threads()
    LOGGER.info("Initializing Docling converter with %d thread(s)", num_threads)
    pipeline_options = PdfPipelineOptions(
        generate_page_images=False,
        generate_picture_images=False,
        generate_table_images=False,
        generate_parsed_pages=False,
        ocr_batch_size=1,
        layout_batch_size=1,
        table_batch_size=1,
        queue_max_size=1,
        accelerator_options=AcceleratorOptions(
            device=AcceleratorDevice.CPU,
            num_threads=num_threads,
        ),
    )
    return DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            )
        },
    )


def get_docling_num_threads() -> int:
    raw_value = os.getenv(DOCLING_THREADS_ENV_VAR, str(DOCLING_DEFAULT_THREADS)).strip()
    try:
        num_threads = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{DOCLING_THREADS_ENV_VAR} must be an integer, got: {raw_value!r}"
        ) from exc

    if num_threads <= 0:
        raise ValueError(f"{DOCLING_THREADS_ENV_VAR} must be > 0, got: {num_threads}")

    return num_threads


def set_docling_num_threads(num_threads: int) -> None:
    global _converter
    if num_threads <= 0:
        raise ValueError(f"Docling thread count must be > 0, got: {num_threads}")

    os.environ[DOCLING_THREADS_ENV_VAR] = str(num_threads)
    with _converter_init_lock:
        _converter = None
    LOGGER.info("Configured Docling thread count: %d", num_threads)


def get_shared_converter() -> DocumentConverter:
    global _converter
    if _converter is None:
        with _converter_init_lock:
            if _converter is None:
                _converter = _build_converter()
    return _converter


def ascii_safe_pdf_filename(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = "".join(
        char if char.isalnum() or char in DOCLING_SAFE_FILENAME_ALLOWED_CHARS else "_"
        for char in ascii_only
    )
    cleaned = " ".join(cleaned.split())
    if not cleaned:
        return DOCLING_PDF_DEFAULT_FILENAME
    if not cleaned.lower().endswith(".pdf"):
        return f"{cleaned}.pdf"
    return cleaned


def _build_staged_pdf_name(pdf_path: Path) -> str:
    ascii_name = ascii_safe_pdf_filename(pdf_path.name)
    stem = Path(ascii_name).stem or Path(DOCLING_PDF_DEFAULT_FILENAME).stem
    suffix = Path(ascii_name).suffix or ".pdf"
    digest = hashlib.sha1(str(pdf_path.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"{stem}__{digest}{suffix}"


def stage_pdf_path_for_pipeline(pdf_path: Path, staging_root: str | Path) -> Path:
    staging_dir = Path(staging_root) / DOCLING_STAGING_DIR_NAME
    staging_dir.mkdir(parents=True, exist_ok=True)

    staged_pdf_path = staging_dir / _build_staged_pdf_name(pdf_path)
    if (
        staged_pdf_path.exists()
        and staged_pdf_path.stat().st_size == pdf_path.stat().st_size
    ):
        return staged_pdf_path

    shutil.copyfile(pdf_path, staged_pdf_path)
    LOGGER.debug("Staged PDF for pipeline: %s -> %s", pdf_path, staged_pdf_path)
    return staged_pdf_path


def stage_pdf_for_docling(
    pdf_path: Path, intermediate_root: str | Path
) -> tuple[Path, Path]:
    temp_dir = Path(
        tempfile.mkdtemp(
            prefix=DOCLING_INTERMEDIATE_PREFIX,
            dir=str(intermediate_root),
        )
    )
    staged_pdf_path = temp_dir / ascii_safe_pdf_filename(pdf_path.name)
    shutil.copyfile(pdf_path, staged_pdf_path)
    return temp_dir, staged_pdf_path


def convert_pdf_to_markdown(
    pdf_path: str | Path,
    *,
    strict_text: bool = False,
    intermediate_root: str | Path = DOCLING_DEFAULT_INTERMEDIATE_ROOT,
) -> str:
    resolved_pdf_path = Path(pdf_path)
    converter = get_shared_converter()
    result = None
    temp_dir = None

    try:
        temp_dir, staged_pdf_path = stage_pdf_for_docling(
            resolved_pdf_path, intermediate_root
        )
        LOGGER.info("Docling conversion started: %s", resolved_pdf_path)
        with _converter_use_lock:
            result = converter.convert(staged_pdf_path)
        markdown = str(result.document.export_to_markdown(strict_text=strict_text))
        LOGGER.info("Docling conversion finished: %s", resolved_pdf_path)
        return markdown
    except Exception as exc:
        LOGGER.warning(
            "Docling conversion failed for %s: %s: %s",
            resolved_pdf_path,
            exc.__class__.__name__,
            exc,
        )
        raise RuntimeError(
            f"Docling conversion failed for {resolved_pdf_path}: {exc}"
        ) from exc
    finally:
        del result
        gc.collect()
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)


def docling_pdf_to_markdown(
    document: dict[str, object],
    input_key: str = DOCLING_DEFAULT_INPUT_KEY,
    output_key: str = DOCLING_DEFAULT_OUTPUT_KEY,
    strict_text: bool = False,
    intermediate_root: str | Path = DOCLING_DEFAULT_INTERMEDIATE_ROOT,
) -> list[dict[str, str]]:
    if input_key not in document:
        raise ValueError(f"Input key {input_key} not found in item: {document}")

    pdf_path = Path(str(document[input_key]))
    filename = document.get(INPUT_FIELD_FILENAME, pdf_path.name)
    LOGGER.info("Preparing Docling conversion for %s", filename)
    markdown = convert_pdf_to_markdown(
        pdf_path,
        strict_text=strict_text,
        intermediate_root=intermediate_root,
    )
    return [{output_key: markdown}]
