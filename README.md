# DocETL Runner

DocETL Runner is a universal command-line tool for executing DocETL workflows against folders of PDF files. It discovers inputs, generates runtime manifests, resolves template placeholders, runs the configured pipeline, and can export structured JSON results to Excel.

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
