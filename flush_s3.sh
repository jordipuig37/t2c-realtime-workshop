#!/bin/bash
cd "$(dirname "$0")"
./realtime-env/bin/python flush_s3.py
echo "$(date +'%Y-%m-%d %H:%M:%S,%3N') - INFO - Finished flush_s3.sh execution" >> logs/cronlogs.log
