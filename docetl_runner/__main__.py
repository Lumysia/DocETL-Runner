"""Entry point for ``python -m docetl_runner``."""

import sys
from pathlib import Path

from docetl_runner.cli import parse_args
from docetl_runner.constants import (
    DOCETL_DIR_NAME,
    INPUT_JSON_SUFFIX,
    INTERMEDIATES_DIR_SUFFIX,
    OUTPUT_JSON_SUFFIX,
    PIPELINE_YAML_SUFFIX,
)
from docetl_runner.discovery import (
    create_input_json,
    discover_pdf_files,
    validate_input_folder,
)
from docetl_runner.excel import convert_json_to_excel
from docetl_runner.log import setup_logging
from docetl_runner.pipeline import resolve_template, run_pipeline, write_pipeline_yaml


def main(argv: list[str] | None = None) -> int:
    """Run the full workflow and return an exit code."""
    args = parse_args(argv)
    logger = setup_logging(verbose=args.verbose)

    input_folder = Path(args.input_folder)
    try:
        validate_input_folder(input_folder)
    except (FileNotFoundError, NotADirectoryError) as exc:
        logger.error(exc)
        return 1

    output_file = (
        Path(args.output)
        if args.output
        else input_folder.parent / f"{input_folder.name}{OUTPUT_JSON_SUFFIX}"
    )

    pipeline_template = Path(args.pipeline)

    try:
        pdf_files = discover_pdf_files(input_folder)
    except ValueError as exc:
        logger.error(exc)
        return 1

    script_dir = Path(__file__).resolve().parent.parent
    docetl_dir = script_dir / DOCETL_DIR_NAME
    folder_name = input_folder.name

    input_json = docetl_dir / f"{folder_name}{INPUT_JSON_SUFFIX}"
    intermediate_dir = docetl_dir / f"{folder_name}{INTERMEDIATES_DIR_SUFFIX}"
    pipeline_yaml_path = docetl_dir / f"{folder_name}{PIPELINE_YAML_SUFFIX}"

    create_input_json(pdf_files, input_json)

    try:
        content = resolve_template(
            pipeline_template, input_json, output_file, intermediate_dir
        )
    except FileNotFoundError as exc:
        logger.error(exc)
        return 1

    write_pipeline_yaml(content, pipeline_yaml_path)

    try:
        run_pipeline(pipeline_yaml_path)
    except ImportError as exc:
        logger.error(exc)
        return 1
    except RuntimeError as exc:
        logger.error(exc)
        return 1

    logger.info("Output saved to %s", output_file)

    if args.excel is not None:
        excel_output = (
            Path(args.excel) if args.excel else output_file.with_suffix(".xlsx")
        )
        logger.info("Converting to Excel: %s → %s", output_file, excel_output)
        try:
            convert_json_to_excel(output_file, excel_output)
        except (FileNotFoundError, ValueError, RuntimeError) as exc:
            logger.error("Excel conversion failed: %s", exc)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
