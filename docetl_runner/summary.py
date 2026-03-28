"""Summary generation for DocETL pipeline results."""

import json
import logging
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from docetl_runner.constants import (
    DEFAULT_SUMMARY_FALLBACK_VALUE,
    FILE_ENCODING,
    INPUT_FIELD_FILENAME,
    SUMMARY_INPUT_COUNT_KEY,
    SUMMARY_INPUT_DOCUMENTS_KEY,
    SUMMARY_INTERMEDIATE_COUNT_KEY,
    SUMMARY_INTERMEDIATE_PATH_KEY,
    SUMMARY_INTERMEDIATE_SUBDIR,
    SUMMARY_MESSAGE_ALL_HAVE_DATA,
    SUMMARY_OUTPUT_COUNT_LABEL,
    SUMMARY_OUTPUT_EMPTY_COUNT_KEY,
    SUMMARY_OUTPUT_EMPTY_DOCUMENTS_KEY,
    SUMMARY_OUTPUT_NESTED_COUNTS_KEY,
    SUMMARY_OUTPUT_RECORD_COUNT_KEY,
    SUMMARY_OUTPUT_TYPE_LABEL,
    SUMMARY_SEPARATOR_WIDTH,
    SUMMARY_TITLE_COMPLETE,
    SUMMARY_TITLE_EMPTY,
    SUMMARY_TITLE_INPUT,
    SUMMARY_TITLE_INTERMEDIATE,
    SUMMARY_TITLE_OUTPUT,
    SUMMARY_TITLE_PANEL,
)

logger = logging.getLogger(__name__)


def _fallback(value: Any) -> str:
    """Return a display-safe fallback string."""
    return str(value or DEFAULT_SUMMARY_FALLBACK_VALUE)


class PipelineSummary:
    """Generate and display pipeline execution summary."""

    def __init__(
        self,
        input_json_path: Path,
        output_file_path: Path,
        intermediate_dir: Path,
    ) -> None:
        """Initialize summary generator.

        Args:
            input_json_path: Path to input JSON file.
            output_file_path: Path to output JSON file.
            intermediate_dir: Path to intermediate directory.
        """
        self.input_json_path = input_json_path
        self.output_file_path = output_file_path
        self.intermediate_dir = intermediate_dir
        self.console = Console()

    def _load_json(self, path: Path) -> list[dict[str, Any]]:
        """Load JSON file safely.

        Args:
            path: Path to JSON file.

        Returns:
            List of dictionaries from JSON file, or empty list if file not found.
        """
        if not path.exists():
            logger.warning("File not found: %s", path)
            return []
        try:
            with open(path, encoding=FILE_ENCODING) as fh:
                data = json.load(fh)
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load %s: %s", path, exc)
            return []

    def _analyze_input(self) -> dict[str, Any]:
        """Analyze input documents.

        Returns:
            Dictionary with input statistics.
        """
        input_data = self._load_json(self.input_json_path)
        return {
            SUMMARY_INPUT_COUNT_KEY: len(input_data),
            SUMMARY_INPUT_DOCUMENTS_KEY: [
                _fallback(d.get(INPUT_FIELD_FILENAME)) for d in input_data
            ],
        }

    def _analyze_output(self) -> dict[str, Any]:
        """Analyze output results.

        Returns:
            Dictionary with output statistics.
        """
        output_data = self._load_json(self.output_file_path)
        empty_results = []
        nested_counts: dict[str, int] = {}

        for doc in output_data:
            per_record_nested_count = 0
            for key, value in doc.items():
                if not isinstance(value, list):
                    continue
                list_items = [item for item in value if isinstance(item, dict)]
                if not list_items:
                    continue
                nested_counts[key] = nested_counts.get(key, 0) + len(list_items)
                per_record_nested_count += len(list_items)

            if per_record_nested_count == 0:
                empty_results.append(_fallback(doc.get(INPUT_FIELD_FILENAME)))

        return {
            SUMMARY_OUTPUT_RECORD_COUNT_KEY: len(output_data),
            SUMMARY_OUTPUT_EMPTY_COUNT_KEY: len(empty_results),
            SUMMARY_OUTPUT_EMPTY_DOCUMENTS_KEY: empty_results,
            SUMMARY_OUTPUT_NESTED_COUNTS_KEY: nested_counts,
        }

    def _analyze_intermediates(self) -> dict[str, Any]:
        """Analyze intermediate processing results.

        Returns:
            Dictionary with intermediate statistics.
        """
        if not self.intermediate_dir.exists():
            return {}

        results = {}
        data_processing_dir = self.intermediate_dir / SUMMARY_INTERMEDIATE_SUBDIR
        if data_processing_dir.exists():
            for json_file in data_processing_dir.glob("*.json"):
                data = self._load_json(json_file)
                results[json_file.stem] = {
                    SUMMARY_INTERMEDIATE_COUNT_KEY: len(data),
                    SUMMARY_INTERMEDIATE_PATH_KEY: str(json_file),
                }
        return results

    def _display_input_summary(self, input_stats: dict[str, Any]) -> None:
        """Display input summary.

        Args:
            input_stats: Input statistics dictionary.
        """
        table = Table(
            title=SUMMARY_TITLE_INPUT, show_header=True, header_style="bold magenta"
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("Filename")

        for i, filename in enumerate(input_stats[SUMMARY_INPUT_DOCUMENTS_KEY], 1):
            table.add_row(str(i), filename)

        self.console.print(table)
        self.console.print(
            "\n[bold green]Total input documents: "
            f"{input_stats[SUMMARY_INPUT_COUNT_KEY]}[/bold green]"
        )

    def _display_output_summary(self, output_stats: dict[str, Any]) -> None:
        """Display output summary.

        Args:
            output_stats: Output statistics dictionary.
        """
        table = Table(
            title=SUMMARY_TITLE_OUTPUT, show_header=True, header_style="bold cyan"
        )
        table.add_column(SUMMARY_OUTPUT_TYPE_LABEL)
        table.add_column(SUMMARY_OUTPUT_COUNT_LABEL)

        total_nested_items = 0
        for field_name, count in sorted(
            output_stats[SUMMARY_OUTPUT_NESTED_COUNTS_KEY].items()
        ):
            table.add_row(field_name, str(count))
            total_nested_items += count
        table.add_row(
            "[bold]Total Nested Items[/bold]",
            f"[bold]{total_nested_items}[/bold]",
        )

        self.console.print(table)

        if output_stats[SUMMARY_OUTPUT_EMPTY_COUNT_KEY] > 0:
            empty_table = Table(
                title=SUMMARY_TITLE_EMPTY,
                show_header=True,
                header_style="bold red",
            )
            empty_table.add_column("#", style="dim", width=4)
            empty_table.add_column("Filename")

            for i, filename in enumerate(
                output_stats[SUMMARY_OUTPUT_EMPTY_DOCUMENTS_KEY], 1
            ):
                empty_table.add_row(str(i), filename)

            self.console.print(empty_table)
            self.console.print(
                "\n[bold red]Warning: "
                f"{output_stats[SUMMARY_OUTPUT_EMPTY_COUNT_KEY]} documents "
                "produced no extracted data[/bold red]"
            )
        else:
            self.console.print(
                f"\n[bold green]{SUMMARY_MESSAGE_ALL_HAVE_DATA}[/bold green]"
            )

        self.console.print(
            "\n[bold green]Total output documents: "
            f"{output_stats[SUMMARY_OUTPUT_RECORD_COUNT_KEY]}[/bold green]"
        )

    def _display_intermediate_summary(self, intermediate_stats: dict[str, Any]) -> None:
        """Display intermediate processing summary.

        Args:
            intermediate_stats: Intermediate statistics dictionary.
        """
        if not intermediate_stats:
            return

        table = Table(
            title=SUMMARY_TITLE_INTERMEDIATE,
            show_header=True,
            header_style="bold yellow",
        )
        table.add_column("Operation")
        table.add_column("Document Count")
        table.add_column("File Path")

        for op_name, stats in intermediate_stats.items():
            table.add_row(
                op_name,
                str(stats[SUMMARY_INTERMEDIATE_COUNT_KEY]),
                stats[SUMMARY_INTERMEDIATE_PATH_KEY],
            )

        self.console.print(table)

    def generate(self) -> None:
        """Generate and display the complete pipeline summary."""
        self.console.print("\n")
        self.console.print(
            Panel.fit(
                f"[bold blue]{SUMMARY_TITLE_PANEL}[/bold blue]",
                border_style="blue",
            )
        )
        self.console.print("\n")

        input_stats = self._analyze_input()
        self._display_input_summary(input_stats)

        self.console.print("\n" + "=" * SUMMARY_SEPARATOR_WIDTH + "\n")

        output_stats = self._analyze_output()
        self._display_output_summary(output_stats)

        self.console.print("\n" + "=" * SUMMARY_SEPARATOR_WIDTH + "\n")

        intermediate_stats = self._analyze_intermediates()
        self._display_intermediate_summary(intermediate_stats)

        self.console.print("\n")
        self.console.print(
            Panel.fit(
                f"[bold green]{SUMMARY_TITLE_COMPLETE}[/bold green]\n"
                f"Output saved to: {self.output_file_path}",
                border_style="green",
            )
        )
        self.console.print("\n")


def generate_summary(
    input_json_path: Path,
    output_file_path: Path,
    intermediate_dir: Path,
) -> None:
    """Generate and display pipeline execution summary.

    Args:
        input_json_path: Path to input JSON file.
        output_file_path: Path to output JSON file.
        intermediate_dir: Path to intermediate directory.
    """
    summary = PipelineSummary(input_json_path, output_file_path, intermediate_dir)
    summary.generate()
