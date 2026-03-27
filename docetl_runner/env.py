"""Environment loading helpers for DocETL and LiteLLM-compatible providers."""

from pathlib import Path

from dotenv import find_dotenv, load_dotenv


def load_project_env() -> Path | None:
    """Load environment variables from the nearest ``.env`` file.

    The search starts from the current working directory so the behavior stays
    universal whether the runner is invoked via ``uv run``, ``python -m``, or an
    installed console script. Existing process environment variables are
    preserved and take precedence over values found in ``.env``.

    Returns:
        The resolved ``.env`` path when one is found and loaded; otherwise
        ``None``.
    """

    env_path = find_dotenv(filename=".env", usecwd=True)
    if not env_path:
        return None

    load_dotenv(env_path, override=False)
    return Path(env_path).resolve()
