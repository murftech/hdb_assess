#!/bin/sh
set -e

source venv/bin/activate

BASE_DIR="$(cd "$(dirname "$0")" && pwd)/datalake"

mkdir -p "$BASE_DIR/raw"
echo "folder datalake/raw created"

mkdir -p "$BASE_DIR/cleaned"
echo "folder datalake/cleaned created"

mkdir -p "$BASE_DIR/transformed"
echo "folder datalake/transformed created"

mkdir -p "$BASE_DIR/failed"
echo "folder datalake/failed created"

mkdir -p "$BASE_DIR/hashed"
echo "folder datalake/hashed created"

mkdir -p "$BASE_DIR/datastore"
echo "folder datalake/datastore created"