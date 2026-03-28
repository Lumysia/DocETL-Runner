"""Pipeline template resolution and DocETL execution."""

import logging
import os
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from docetl_runner.constants import (
    DEFAULT_WINDOWS_UTF8_ENV_VARS,
    FILE_ENCODING,
    PATH_DISPLAY_SEPARATOR,
    PIPELINE_PROGRESS_DESCRIPTION,
    TEMPLATE_PLACEHOLDER_INPUT,
    TEMPLATE_PLACEHOLDER_INTERMEDIATE,
    TEMPLATE_PLACEHOLDER_OUTPUT,
)

logger = logging.getLogger(__name__)


def configure_runtime_environment() -> None:
    for name, value in DEFAULT_WINDOWS_UTF8_ENV_VARS:
        if os.environ.get(name) != value:
            os.environ[name] = value
            logger.debug("Set runtime environment %s=%s", name, value)


def _posix_str(path: Path) -> str:
    """Convert a Path to a forward-slash string suitable for YAML."""
    return (
        path.as_posix()
        if hasattr(path, "as_posix")
        else str(path).replace("\\", PATH_DISPLAY_SEPARATOR)
    )


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
    configure_runtime_environment()

    try:
        from docetl.runner import DSLRunner
    except ImportError as exc:
        raise ImportError(
            "docetl package not found. Install it with: pip install docetl"
        ) from exc

    logger.info("Starting DocETL pipeline from %s", pipeline_yaml_path)

    with Progress(
        SpinnerColumn(),
        TextColumn(PIPELINE_PROGRESS_DESCRIPTION),
        TimeElapsedColumn(),
        transient=False,
        console=None,
    ) as progress:
        task = progress.add_task("pipeline", total=None)
        try:
            runner = DSLRunner.from_yaml(str(pipeline_yaml_path))
            runner.load_run_save()
            progress.update(task, completed=1, total=1)
        except Exception as exc:
            raise RuntimeError(f"Pipeline execution failed: {exc}") from exc

    logger.info("Pipeline completed successfully")
