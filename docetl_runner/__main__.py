"""Entry point for ``python -m docetl_runner``."""

import json
import logging
import sys
from pathlib import Path

from docetl_runner.cli import parse_args
from docetl_runner.constants import (
    BATCHES_DIR_NAME,
    DEFAULT_EXIT_CODE_FAILURE,
    DEFAULT_EXIT_CODE_SUCCESS,
    DOCETL_DIR_NAME,
    FILE_ENCODING,
    INPUT_JSON_SUFFIX,
    INTERMEDIATE_DIR_SUFFIX,
    INTERMEDIATES_DIR_SUFFIX,
    OUTPUT_JSON_SUFFIX,
    PIPELINE_YAML_SUFFIX,
)
from docetl_runner.discovery import (
    create_batched_input_json,
    create_input_json,
    discover_pdf_files,
    validate_input_folder,
)
from docetl_runner.docling import set_docling_num_threads
from docetl_runner.env import load_project_env
from docetl_runner.excel import convert_json_to_excel
from docetl_runner.log import setup_logging
from docetl_runner.pipeline import resolve_template, run_pipeline, write_pipeline_yaml
from docetl_runner.summary import generate_summary

logger = logging.getLogger(__name__)


def merge_batch_outputs(batch_output_files: list[Path], output_file: Path) -> None:
    """Merge multiple batch output JSON files into a single output file.

    Args:
        batch_output_files: List of batch output JSON file paths.
        output_file: Destination for the merged output file.
    """
    merged_records = []

    for batch_file in batch_output_files:
        try:
            with open(batch_file, encoding=FILE_ENCODING) as fh:
                records = json.load(fh)
                if isinstance(records, list):
                    merged_records.extend(records)
                else:
                    logger.warning(
                        "Batch file %s does not contain a list, skipping", batch_file
                    )
        except (json.JSONDecodeError, FileNotFoundError) as exc:
            logger.warning("Failed to read batch file %s: %s", batch_file, exc)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding=FILE_ENCODING) as fh:
        json.dump(merged_records, fh, ensure_ascii=False, indent=2)

    logger.info("Merged %d batch file(s) into %s", len(batch_output_files), output_file)


def main(argv: list[str] | None = None) -> int:
    """Run the full workflow and return an exit code."""
    args = parse_args(argv)
    logger = setup_logging(verbose=args.verbose)
    env_path = load_project_env()
    if env_path is not None:
        logger.debug("Loaded environment variables from %s", env_path)

    if args.docling_threads is not None:
        set_docling_num_threads(args.docling_threads)
        logger.info("Using Docling thread override from CLI: %d", args.docling_threads)

    input_folder = Path(args.input_folder)
    try:
        validate_input_folder(input_folder)
    except (FileNotFoundError, NotADirectoryError) as exc:
        logger.error(exc)
        return DEFAULT_EXIT_CODE_FAILURE

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
        return DEFAULT_EXIT_CODE_FAILURE

    script_dir = Path(__file__).resolve().parent.parent
    docetl_dir = script_dir / DOCETL_DIR_NAME
    folder_name = input_folder.name

    if args.batch_size is not None and args.batch_size > 0:
        logger.info(
            "Batch processing enabled: processing %d PDFs in batches of %d",
            len(pdf_files),
            args.batch_size,
        )

        batch_input_dir = (
            docetl_dir / f"{folder_name}{INTERMEDIATES_DIR_SUFFIX}" / BATCHES_DIR_NAME
        )
        batch_input_files = create_batched_input_json(
            pdf_files, batch_input_dir, folder_name, args.batch_size
        )

        batch_output_files = []
        failed_batches = []

        for batch_num, batch_input_file in enumerate(batch_input_files, start=1):
            logger.info(
                "Processing batch %d/%d: %s",
                batch_num,
                len(batch_input_files),
                batch_input_file.name,
            )

            batch_output_file = (
                batch_input_dir / f"{batch_input_file.stem}{OUTPUT_JSON_SUFFIX}"
            )
            batch_intermediate_dir = (
                batch_input_dir / f"{batch_input_file.stem}{INTERMEDIATE_DIR_SUFFIX}"
            )
            batch_pipeline_yaml_path = (
                batch_input_dir / f"{batch_input_file.stem}{PIPELINE_YAML_SUFFIX}"
            )

            try:
                content = resolve_template(
                    pipeline_template,
                    batch_input_file,
                    batch_output_file,
                    batch_intermediate_dir,
                )
            except FileNotFoundError as exc:
                logger.error(
                    "Failed to resolve template for batch %d: %s", batch_num, exc
                )
                failed_batches.append(batch_num)
                continue

            write_pipeline_yaml(content, batch_pipeline_yaml_path)

            try:
                run_pipeline(batch_pipeline_yaml_path)
            except ImportError as exc:
                logger.error("Failed to run pipeline for batch %d: %s", batch_num, exc)
                failed_batches.append(batch_num)
                continue
            except RuntimeError as exc:
                logger.error("Failed to run pipeline for batch %d: %s", batch_num, exc)
                failed_batches.append(batch_num)
                continue

            if batch_output_file.exists():
                batch_output_files.append(batch_output_file)
                logger.info(
                    "Batch %d/%d completed successfully",
                    batch_num,
                    len(batch_input_files),
                )
            else:
                logger.warning(
                    "Batch %d/%d completed but output file not found: %s",
                    batch_num,
                    len(batch_input_files),
                    batch_output_file,
                )
                failed_batches.append(batch_num)

        if batch_output_files:
            logger.info("Merging %d batch output file(s)", len(batch_output_files))
            merge_batch_outputs(batch_output_files, output_file)
        else:
            logger.error("No batch output files to merge")
            return DEFAULT_EXIT_CODE_FAILURE

        if failed_batches:
            logger.warning(
                "Failed to process %d batch(es): %s",
                len(failed_batches),
                ", ".join(map(str, failed_batches)),
            )

        logger.info("Cleaning up intermediate batch files...")
        for batch_file in batch_input_files:
            try:
                batch_file.unlink()
            except OSError as exc:
                logger.warning("Failed to delete batch file %s: %s", batch_file, exc)

        for batch_output_file in batch_output_files:
            try:
                batch_output_file.unlink()
            except OSError as exc:
                logger.warning(
                    "Failed to delete batch output file %s: %s", batch_output_file, exc
                )

        for batch_yaml_path in batch_input_dir.glob(f"*{PIPELINE_YAML_SUFFIX}"):
            try:
                batch_yaml_path.unlink()
            except OSError as exc:
                logger.warning(
                    "Failed to delete batch pipeline file %s: %s", batch_yaml_path, exc
                )

        try:
            if batch_input_dir.exists() and not any(batch_input_dir.iterdir()):
                batch_input_dir.rmdir()
                logger.info("Removed empty batch directory: %s", batch_input_dir)
        except OSError as exc:
            logger.warning(
                "Failed to remove batch directory %s: %s", batch_input_dir, exc
            )

    else:
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
            return DEFAULT_EXIT_CODE_FAILURE

        write_pipeline_yaml(content, pipeline_yaml_path)

        try:
            run_pipeline(pipeline_yaml_path)
        except ImportError as exc:
            logger.error(exc)
            return DEFAULT_EXIT_CODE_FAILURE
        except RuntimeError as exc:
            logger.error(exc)
            return DEFAULT_EXIT_CODE_FAILURE

        logger.info("Output saved to %s", output_file)

    if not args.no_summary and args.batch_size is None:
        try:
            generate_summary(input_json, output_file, intermediate_dir)
        except Exception as exc:
            logger.warning("Failed to generate summary: %s", exc)
    elif not args.no_summary and args.batch_size is not None:
        logger.info("Skipping summary generation for batch processing")

    if args.excel is not None:
        excel_output = (
            Path(args.excel) if args.excel else output_file.with_suffix(".xlsx")
        )
        logger.info("Converting to Excel: %s → %s", output_file, excel_output)
        try:
            convert_json_to_excel(output_file, excel_output)
        except (FileNotFoundError, ValueError, RuntimeError) as exc:
            logger.error("Excel conversion failed: %s", exc)
            return DEFAULT_EXIT_CODE_FAILURE

    return DEFAULT_EXIT_CODE_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
