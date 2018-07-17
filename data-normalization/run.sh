#!/usr/bin/env bash

set -eE

.venv/bin/python pipeline.py
.venv/bin/python combine.py
