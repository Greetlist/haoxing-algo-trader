#!/bin/bash
today_str=$(date +%Y%m%d)
mkdir -p input/$today_str/
mkdir -p output/$today_str/
/opt/miniconda/envs/stock_dev/bin/python main.py start
