#!/bin/sh
set -eu
python -m compileall -q metrology demo_support
python -m unittest discover -s tests -v
python -m metrology demo
python demo_support/check.py
