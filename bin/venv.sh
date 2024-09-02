#!/usr/bin/env bash
set -e
python3.12 -m venv .env
. .env/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements.txt

deactivate
