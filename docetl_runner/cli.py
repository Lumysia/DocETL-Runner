"""Command-line interface definition for the universal DocETL runner."""

import argparse


def build_parser() -> argparse.ArgumentParser:
    """Construct and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Run a DocETL workflow on a folder of PDF files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m docetl_runner input_docs -p pipeline_template.yaml\n"
            "  python -m docetl_runner input_docs "
            "-p pipeline_template.yaml -o output.json\n"
            "  python -m docetl_runner input_docs "
            "-p pipeline_template.yaml -e results.xlsx\n"
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
            "Pipeline YAML template file; must contain {{INPUT_JSON}}, "
            "{{OUTPUT_FILE}}, and {{INTERMEDIATE_DIR}}"
        ),
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Output JSON file path (default: <input_folder>_output.json)",
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

    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Explicit argument list (uses ``sys.argv`` when *None*).
    """
    return build_parser().parse_args(argv)
