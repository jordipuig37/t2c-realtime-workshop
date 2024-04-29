#!/bin/bash
cd "$(dirname "$0")"
./realtime-env/bin/python flush_s3.py
