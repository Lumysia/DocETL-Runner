# DocETL Runner

DocETL Runner is a universal command-line tool for executing DocETL workflows against folders of PDF files. It discovers inputs, generates runtime manifests, resolves template placeholders, runs the configured pipeline, and can export structured JSON results to Excel.

## Environment Variables

The runner automatically loads the nearest [`.env`](.env) file starting from the current working directory. This makes provider credentials available to DocETL / LiteLLM without hard-coding any provider-specific logic in the runner.

- Existing shell environment variables are preserved and take precedence.
- Values from [`.env`](.env) are transparently passed through to DocETL and LiteLLM.
- This works for OpenRouter and any other provider DocETL / LiteLLM supports via environment variables.

Example [`.env`](.env):

```env
OPENROUTER_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here
```

You can also start from [`.env.example`](.env.example).

## Usage

```bash
python main.py <input_folder> -p <pipeline_template>
python -m docetl_runner <input_folder> -p <pipeline_template>
docetl-runner <input_folder> -p <pipeline_template>
```

## Features

- Universal package structure
- Structured logging with Rich output
- Progress indicators for pipeline execution and Excel export
- Runtime template resolution for DocETL workflows
- Optional JSON-to-Excel conversion
