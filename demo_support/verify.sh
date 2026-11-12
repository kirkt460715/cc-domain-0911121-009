#!/bin/sh
set -eu
python -m compileall -q metrology demo_support tests
python -m unittest discover -s tests -v
python -m metrology demo
python demo_support/check.py
python -I demo_support/drill_gateway.py
