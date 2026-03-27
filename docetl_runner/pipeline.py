"""Pipeline template resolution and DocETL execution."""

import logging
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from docetl_runner.constants import (
    FILE_ENCODING,
    TEMPLATE_PLACEHOLDER_INPUT,
    TEMPLATE_PLACEHOLDER_INTERMEDIATE,
    TEMPLATE_PLACEHOLDER_OUTPUT,
)

logger = logging.getLogger(__name__)


def _posix_str(path: Path) -> str:
    """Convert a Path to a forward-slash string suitable for YAML."""
    return str(path).replace("\\", "/")


def resolve_template(
    template_path: Path,
    input_json_path: Path,
    output_file_path: Path,
    intermediate_dir: Path,
) -> str:
    """Load a YAML template and replace placeholders with resolved paths.

    Args:
        template_path: Path to the YAML template.
        input_json_path: Replaces ``{{INPUT_JSON}}``.
        output_file_path: Replaces ``{{OUTPUT_FILE}}``.
        intermediate_dir: Replaces ``{{INTERMEDIATE_DIR}}``.

    Returns:
        Resolved YAML content.

    Raises:
        FileNotFoundError: Template does not exist.
    """
    if not template_path.exists():
        raise FileNotFoundError(f"Pipeline template file not found: {template_path}")

    with open(template_path, encoding=FILE_ENCODING) as fh:
        content = fh.read()

    replacements = {
        TEMPLATE_PLACEHOLDER_INPUT: _posix_str(input_json_path),
        TEMPLATE_PLACEHOLDER_OUTPUT: _posix_str(output_file_path),
        TEMPLATE_PLACEHOLDER_INTERMEDIATE: _posix_str(intermediate_dir),
    }
    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)

    logger.debug("Resolved template placeholders in %s", template_path)
    return content


def write_pipeline_yaml(content: str, dest: Path) -> None:
    """Persist resolved pipeline YAML to *dest*."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding=FILE_ENCODING) as fh:
        fh.write(content)
    logger.info("Wrote pipeline YAML: %s", dest)


def run_pipeline(pipeline_yaml_path: Path) -> None:
    """Execute the DocETL pipeline with a Rich spinner.

    Args:
        pipeline_yaml_path: Path to the resolved pipeline YAML.

    Raises:
        ImportError: ``docetl`` is not installed.
        RuntimeError: Pipeline execution failed.
    """
    try:
        from docetl.runner import DSLRunner
    except ImportError as exc:
        raise ImportError(
            "docetl package not found. Install it with: pip install docetl"
        ) from exc

    logger.info("Starting DocETL pipeline from %s", pipeline_yaml_path)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]Running pipeline…"),
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
        progress.add_task("pipeline", total=None)
        try:
            runner = DSLRunner.from_yaml(str(pipeline_yaml_path))
            runner.load_run_save()
        except Exception as exc:
            raise RuntimeError(f"Pipeline execution failed: {exc}") from exc

    logger.info("Pipeline completed successfully")
