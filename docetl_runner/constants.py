"""Centralized constants for the universal DocETL runner."""

DEFAULT_PRIMARY_METADATA_COLUMNS: tuple[str, ...] = (
    "filename",
    "document_id",
    "title",
)

PDF_GLOB_PATTERN = "*.pdf"
FILE_ENCODING = "utf-8"

EXCEL_SHEET_NAME_MAX_LENGTH = 31
EXCEL_ENGINE = "openpyxl"

NULL_STRING = "null"

TEMPLATE_PLACEHOLDER_INPUT = "{{INPUT_JSON}}"
TEMPLATE_PLACEHOLDER_OUTPUT = "{{OUTPUT_FILE}}"
TEMPLATE_PLACEHOLDER_INTERMEDIATE = "{{INTERMEDIATE_DIR}}"

DOCETL_DIR_NAME = "docetl"
INPUT_JSON_SUFFIX = "_input.json"
OUTPUT_JSON_SUFFIX = "_output.json"
INTERMEDIATES_DIR_SUFFIX = "_intermediates"
PIPELINE_YAML_SUFFIX = "_pipeline.yaml"

INPUT_FIELD_FILENAME = "filename"
INPUT_FIELD_PDF_PATH = "pdf_path"
