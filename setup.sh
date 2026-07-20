#!/bin/sh
set -e


# type in terminal if permission denied
# chmod +x startup.sh


# requires 3.13 please install 3.13
python3.13 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
