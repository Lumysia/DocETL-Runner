#!/usr/bin/env python3
"""Thin entry point that delegates to the universal DocETL runner package."""

import sys

from docetl_runner.__main__ import main

if __name__ == "__main__":
    sys.exit(main())
