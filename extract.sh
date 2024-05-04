#!/bin/bash
cd "$(dirname "$0")"
./realtime-env/bin/python extract.py
echo "$(date +'%Y-%m-%d %H:%M:%S,%3N') - INFO - Finished extract.sh execution" >> logs/cronlogs.log
