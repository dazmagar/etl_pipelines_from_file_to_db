#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$BASH_SOURCE")")

bash "$SCRIPT_DIR/stop_db.sh"
bash "$SCRIPT_DIR/clean_db.sh"
bash "$SCRIPT_DIR/start_db.sh"