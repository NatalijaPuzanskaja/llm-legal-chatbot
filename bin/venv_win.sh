#!/usr/bin/env bash
set -e
python -m venv .env
. .env/Scripts/activate
pip install --upgrade pip
pip install -r requirements.txt

deactivate
