"""Command-line interface definition for the universal DocETL runner."""

import argparse

from docetl_runner.constants import (
    CLI_BATCH_HELP_TEMPLATE,
    CLI_DEFAULT_OUTPUT_TEMPLATE,
    CLI_EXAMPLE_INPUT_FOLDER,
    CLI_EXAMPLE_PIPELINE_TEMPLATE,
    TEMPLATE_PLACEHOLDER_INPUT,
    TEMPLATE_PLACEHOLDER_INTERMEDIATE,
    TEMPLATE_PLACEHOLDER_OUTPUT,
)


def build_parser() -> argparse.ArgumentParser:
    """Construct and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Run a DocETL workflow on a folder of PDF files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m docetl_runner "
            f"{CLI_EXAMPLE_INPUT_FOLDER} -p {CLI_EXAMPLE_PIPELINE_TEMPLATE}\n"
            f"  python -m docetl_runner {CLI_EXAMPLE_INPUT_FOLDER} "
            f"-p {CLI_EXAMPLE_PIPELINE_TEMPLATE} -o output.json\n"
            f"  python -m docetl_runner {CLI_EXAMPLE_INPUT_FOLDER} "
            f"-p {CLI_EXAMPLE_PIPELINE_TEMPLATE} -e results.xlsx\n"
        ),
    )

    parser.add_argument(
        "input_folder",
        type=str,
        help="Path to the folder containing PDF files",
    )

    parser.add_argument(
        "-p",
        "--pipeline",
        type=str,
        required=True,
        help=(
            f"Pipeline YAML template file; must contain {TEMPLATE_PLACEHOLDER_INPUT}, "
            f"{TEMPLATE_PLACEHOLDER_OUTPUT}, and {TEMPLATE_PLACEHOLDER_INTERMEDIATE}"
        ),
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help=f"Output JSON file path (default: {CLI_DEFAULT_OUTPUT_TEMPLATE})",
    )

    parser.add_argument(
        "-e",
        "--excel",
        type=str,
        nargs="?",
        const="",
        default=None,
        help="Convert the output JSON to Excel; optionally specify the output path",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Enable debug logging",
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        default=False,
        help="Disable automatic summary generation after pipeline execution",
    )

    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=None,
        help=CLI_BATCH_HELP_TEMPLATE,
    )

    parser.add_argument(
        "--docling-threads",
        type=int,
        default=None,
        help="Override Docling CPU thread count for PDF parsing",
    )

    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Explicit argument list (uses ``sys.argv`` when *None*).
    """
    return build_parser().parse_args(argv)
