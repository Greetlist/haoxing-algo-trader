from HPI import StockTask, HttpClient, AlgoType, TradeType, QtyType
from spi import TraderCallback
import os
import pandas as pd
import time
import signal
import datetime as dt
import sys
import logging
import subprocess as sub

class HPITrader:
    def __init__(self, config):
        self.config = config
        self.today_str = dt.datetime.now().strftime("%Y%m%d")
        self.init_logger()
        self.targetpos_filename = self.config["target_pos_file_template"].format(self.today_str)
        self.idx_filename = self.config["idx_file_template"].format(self.today_str, self.today_str)

        self.stop = False
        self.last_change_time = 0
        self.start_order_index = self.reload_order_index()
        self.init_signal_catch()

        self.trade_api = HttpClient(
            self.config["server_addr"],
            user=self.config["trader_user"],
            password=self.config["trader_password"],
        )
        login_res = self.trade_api.login()
        self.logger.info(login_res)
        if login_res["reset_password"] == 1:
            self.logger.info("Need Reset Password")
            self.trade_api.update_password(self.config["trader_password"], self.config["trader_password"])
        self.cli_token = login_res["token"]

        trade_account_list = self.trade_api.get_tradeaccount()
        for item in trade_account_list:
            self.logger.info("trade account: ", item)
            if item["account_id"] == config["fund_account"]:
                if item["status"] == 0:
                    self.logger.info("Need Activate Account")
                    self.trade_api.activate_account(config["fund_account"], config["fund_account_password"])
                self.account_id = config["fund_account"]

        sys.exit(1)
        self.call_back = TraderCallback(self.config, token)
        self.call_back.init()
        self.call_back.start()

    def init_logger(self):
        sub.check_call("mkdir -p {}".format(self.config["log_base_dir"]), shell=True)
        log_file_name = self.config["log_name_template"].format(self.today_str)
        self.logger = logging.getLogger(log_file_name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            formatter = logging.Formatter("[%(asctime)s] | %(levelname)s | [%(filename)s:%(lineno)s] | %(message)s")
            file_handler = logging.FileHandler(os.path.join(self.config["log_base_dir"], log_file_name))
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def reload_order_index(self):
        if os.path.exists(self.idx_filename):
            with open(self.idx_filename, "r") as f:
                data = f.read()
                return int(data)
        return 0

    def init_signal_catch(self):
        signal.signal(45, self.stop_signal_handler)

    def stop_signal_handler(self):
        self.stop = True

    def start(self):
        while not self.stop:
            if os.path.exists(self.targetpos_filename):
                file_stat = os.stat(self.targetpos_filename)
                self.logger.info(file_stat.st_ctime)
                if file_stat.st_ctime <= self.last_change_time:
                    self.logger.info("TargetPos File not change, continue")
                    time.sleep(1)
                    continue

                target_pos_df = pd.read_csv(self.targetpos_filename)[self.start_order_index:]
                task_list = self.gen_total_task_list(target_pos_df)
                if len(task_list) > 0:
                    self.logger.info(task_list)
                    #res = self.trade_api.create_tasks(task_list)
                    #self.logger.info(res)
                    if True:
                        self.start_order_index += len(task_list)
                        self.save_idx()
            time.sleep(1)

    def gen_total_task_list(self, df):
        task_list = []
        for item in df.to_dict("records"):
            time_list = item["ExtraData"].split(";")
            if len(time_list) < 2:
                self.logger.info("Invalid record item: ", item)
                return []
            start_time_str, end_time_str = time_list[0], time_list[1]
            single_task = StockTask(
                account_id=self.account_id,
                start_time=self.convert_time(start_time_str),
                end_time=self.convert_time(end_time_str),
                symbol=self.convert_exchange(item["ExchangeID"]),
                algo_name=self.config["algo_name"],
                task_qty=item["Qty"],
                trade_type=self.convert_direction(item["Side"]),
                qty_type=QtyType.DELTA
            )
            task_list.append(single_task)
        return task_list

    def convert_exchange(self, origin_exchange):
        return "XSHE" if origin_exchange == "SZ" else "XSHG"

    def convert_direction(self, origin_direction):
        return TradeType.NORMAL_BUY if origin_direction == "Buy" else TradeType.NORMAL_SELL

    def convert_time(self, time_str):
        d = dt.datetime.strptime(time_str, "%H:%M:%S")
        self.logger.info(d.hour, d.minute, d.second)
        return d.hour * 10000 + d.minute * 100 + d.second

    def save_idx(self):
        with open(self.idx_filename, "w") as f:
            f.write(str(self.start_order_index))
            f.close()
