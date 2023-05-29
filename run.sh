#!/bin/bash
today_str=$(date +%Y%m%d)
mkdir -p input/$today_str/
mkdir -p output/$today_str/

if [[ -f "input/${today_str}_order.csv" ]]; then
    echo "order file has existed"
else
    touch input/${today_str}_order.csv
    echo "OrderId,InstrumentID,ExchangeID,Side,Qty,ExtraData" >>input/${today_str}_order.csv
fi

/opt/miniconda/envs/stock_dev/bin/python main.py start
