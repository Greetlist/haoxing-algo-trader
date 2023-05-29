from HPI import StockTask, HttpClient, AlgoType, TradeType, QtyType
from spi import TraderCallback
from query import HPIQuery
import os
import pandas as pd
import time
import signal
import datetime as dt
import sys
import logging
import subprocess as sub
import threading
from threading import Lock
import copy
import math

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
        self.order_map_mutex = Lock()
        self.sys_id_local_id_map = self.reload_order_id_map()

        self.init_signal_catch()
        self.init_trade_api()
        self.init_query_thread()

    def init_trade_api(self):
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
            self.logger.info("trade account: {}".format(item))
            if item["account_id"] == self.config["fund_account"]:
                if item["status"] == 0:
                    self.logger.info("Need Activate Account")
                    self.trade_api.activate_account(self.config["fund_account"], self.config["fund_account_password"])
                self.account_id = self.config["fund_account"]
        self.callback_thread = threading.Thread(target=self.start_callback)
        self.callback_thread.start()

    def init_query_thread(self):
        self.query_api = HPIQuery(self.config, self)
        self.query_thread = threading.Thread(target=self.start_query)
        self.query_thread.start()

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

    def reload_order_id_map(self):
        res = dict()
        order_csv_filename = self.config["output_order_template"].format(self.today_str)
        if not os.path.exists(order_csv_filename):
            return res
        df = pd.read_csv(order_csv_filename, usecols=["OrderId", "OrderSysId"], dtype=str)
        for item in df.to_dict("records"):
            res[item["OrderSysId"]] = item["OrderId"]
        return res

    def init_signal_catch(self):
        signal.signal(45, self.stop_signal_handler)

    def stop_signal_handler(self):
        self.stop = True

    def start_callback(self):
        self.call_back = TraderCallback(self.config, self.cli_token, self)
        self.call_back.init()
        self.call_back.start()

    def start_query(self):
        while not self.stop:
            self.order_map_mutex.acquire()
            self.query_api.query("trade")
            self.query_api.query("sub_order")
            self.query_api.query_position()
            self.order_map_mutex.release()
            time.sleep(int(self.config["query_interval"]))

    def start(self):
        while not self.stop:
            time.sleep(1)
            if os.path.exists(self.targetpos_filename):
                file_stat = os.stat(self.targetpos_filename)
                if file_stat.st_ctime <= self.last_change_time:
                    self.logger.info("FileChangeTime: {}, LastChangeTime: {}, Skip".format(file_stat.st_ctime , self.last_change_time))
                    continue

                target_pos_df = pd.read_csv(
                    self.targetpos_filename,
                    dtype={"OrderId":str, "InstrumentID":str})[self.start_order_index:]
                task_list = self.gen_total_task_list(target_pos_df)
                self.last_change_time = file_stat.st_ctime
                if len(task_list) <= 0:
                    continue

                self.logger.info("Start to Send Orders")
                self.order_map_mutex.acquire()
                self.batch_send_order(target_pos_df, task_list)
                self.order_map_mutex.release()

    def gen_total_task_list(self, df):
        task_list = []
        for item in df.to_dict("records"):
            time_list = item["ExtraData"].split(";")
            if len(time_list) < 2:
                self.logger.info("Invalid record item: {}".format(item))
                return []
            start_time_str, end_time_str = time_list[0], time_list[1]
            single_task = StockTask(
                account_id=self.account_id,
                start_time=self.convert_time(start_time_str),
                end_time=self.convert_time(end_time_str),
                symbol=self.convert_exchange(item["ExchangeID"])+str(item["InstrumentID"]),
                algo_name=self.config["algo_name"],
                task_qty=int(item["Qty"]),
                trade_type=self.convert_direction(item["Side"]),
                qty_type=QtyType.DELTA
            )
            task_list.append(single_task)
        return task_list

    def batch_send_order(self, target_pos_df, total_task_list):
        send_per_round = int(self.config["send_order_per_round"])
        send_round = math.ceil(len(total_task_list) / send_per_round)
        total_res_list = []
        for i in range(send_round):
            task_list = total_task_list[i*send_per_round:(i+1)*send_per_round]
            res = self.trade_api.create_tasks(task_list)
            self.logger.info(res)
            total_res_list.extend(res["datas"])

            if res["error_code"] == 0:
                self.start_order_index += len(task_list)
                self.save_idx()
            else:
                self.logger.error("Batch Send Order Error, error_msg is: {}".format(res["message"]))
        self.dump_origin_order(target_pos_df.to_dict("records"), total_res_list)

    def dump_origin_order(self, target_pos_list, res_list):
        assert len(target_pos_list) == len(res_list), "TargetPos Length != ResList Length"
        order_csv_filename = self.config["output_order_template"].format(self.today_str)
        order_list = [] \
            if not os.path.exists(order_csv_filename) \
            else pd.read_csv(order_csv_filename, dtype={"OrderID":str, "InstrumentID":str}).to_dict('records')
        for idx in range(len(target_pos_list)):
            target_pos_item = target_pos_list[idx]
            res_item = res_list[idx]
            self.sys_id_local_id_map[str(res_item["client_task_id"])] = target_pos_item["OrderId"]
            order_list.append(self.gen_order_item(target_pos_item, res_item))
        if len(order_list) == 0:
            return

        order_df = pd.DataFrame(order_list)
        order_df.to_csv(order_csv_filename, index=False)

    def gen_order_item(self, target_pos_item, res_item):
        res = dict()
        res["AccountUid"] = self.account_id
        res["OrderId"] = target_pos_item["OrderId"]
        res["OrderSysId"] = res_item["client_task_id"]
        res["OrderStatus"] = "Rejected" if res_item["error_code"] != 0 else "Accepted"
        res["InstrumentID"] = target_pos_item["InstrumentID"]
        res["Exchange"] = target_pos_item["ExchangeID"]
        res["OrderSide"] = target_pos_item["Side"]
        res["OrderPrice"] = 0
        res["OrderSize"] = target_pos_item["Qty"]
        res["OrderRemainSize"] = target_pos_item["Qty"]
        res["OrderTime"] = dt.datetime.now().strftime("%Y%m%d%H%M%S")
        res["ErrorMsg"] = res_item["message"] if res_item["error_code"] != 0 else ""
        return res

    def convert_exchange(self, origin_exchange):
        return "XSHE" if origin_exchange == "SZ" else "XSHG"

    def convert_direction(self, origin_direction):
        return TradeType.NORMAL_BUY if origin_direction == "Buy" else TradeType.NORMAL_SELL

    def convert_time(self, time_str):
        d = dt.datetime.strptime(time_str, "%H:%M:%S")
        return d.hour * 10000 + d.minute * 100 + d.second

    def save_idx(self):
        with open(self.idx_filename, "w") as f:
            f.write(str(self.start_order_index))
            f.close()
