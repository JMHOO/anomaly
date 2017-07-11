#!/bin/bash

python3 ./src/detection_main.py --batch-log=./log_input/batch_log.json --stream-log=./log_input/stream_log.json --output=./log_output/flagged_purchases.json
