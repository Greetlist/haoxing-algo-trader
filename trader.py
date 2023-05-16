from HPI import StockTask, HttpClient, AlgoType, TradeType, QtyType
from spi import TraderCallback
import os
import pandas as pd
import time
import signal

class HPITrader:
    def __init__(self, config):
        self.config = config
        self.trade_api = HttpClient(
            config["server_addr"],
            user=config["user"],
            password=config["password"],
        )

        self.stop = False
        self.last_change_time = time.time()
        self.start_order_idx = self.reload_order_index()
        self.call_back = TraderCallback(config)
        self.call_back.init()
        self.call_back.start()

        self.init_signal_catch()

    def reload_order_index(self):
        if os.path.exists(config["idx_file"]):
            with open(config["idx_file"], "r") as f:
                data = f.read()
                return int(data)
        return 0

    def init_signal_catch(self):
        signal.signal(45, stop_signal_handler)

    def stop_signal_handler(self):
        self.stop = True

    def start(self):
        while not self.stop:
            if os.path.exists(config["target_pos_file"]):
                file_stat = os.stat(config["target_pos_file"])
                print(file_stat.st_ctime)
                if file_stat.st_ctime <= self.last_change_time:
                    print("TargetPos File not change, continue")
                    continue

                target_pos_df = pd.read_csv(config["target_pos_file"])[self.start_order_index:]
                task_list = self.gen_total_task_list(target_pos_df)
                if len(task_list) > 0:
                    res = self.trade_api.create_tasks(task_list)
                    print(res)
            time.sleep(1)

    def gen_total_task_list(self, df):
        task_list = []
        for item in df.to_dict("records"):
            time_list = item["ExtraData"].split(";")
            if len(time_list) < 2:
                print("Invalid record item: ", item)
                return []
            start_time_str, end_time_str = time_list[0], time_list[1]
            single_task = StockTask(
                account_id=config["account_id"],
                start_time=self.conver_time(start_time_str),
                end_time=self.conver_time(end_time_str),
                symbol=convert_exchange(item["ExchangeID"]),
                algo_name=config["algo_name"],
                task_qty=item["Qty"],
                trade_type=self.convert_direction(item["Side"]),
                qty_type=QtyType.DELTA
            )
            task_list.append(single_task)
        return task_list

    def convert_exchange(self, origin_exchange):
        return "XSHE" if origin_exchange == "SZ" else "XSHG"

    def convert_direction(self, origin_direction):
        return TradeType.Normal_BUY if origin_direction == "Buy" else TradeType.Normal_SELL

    def convert_time(self, time_str):
        d = dt.datetime.strptime(time_str, "%H:%m:%s")
        return d.hour * 100000 + d.minute * 1000 + d.second
