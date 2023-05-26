from HPI import StockTask, HttpClient, AlgoType, TradeType, QtyType
from HPI.constants import OrderStatus
import argh
import configparser
import datetime as dt
import pandas as pd
import subprocess as sub

class HPIQuery:
    def __init__(self, config, trader):
        self.config = config
        self.trader = trader
        self.today_str = dt.datetime.now().strftime("%Y%m%d")
        self.trade_api = HttpClient(
            self.config["server_addr"],
            user=self.config["trader_user"],
            password=self.config["trader_password"],
        )
        login_res = self.trade_api.login()
        print(login_res)
        self.account_id = config["fund_account"]
        self.query_num = int(config["query_num"])

        self.trade_csv_column = [
            "AccountUid", "OrderId", "OrderSysId",
            "InstrumentId", "Exchange", "OrderSide",
            "FillSize", "FillPrice",
            "TradeStatus", "TradeTime"
        ]

    def query(self, ftype):
        start_page = 1
        total_convert_list = []
        while True:
            if ftype == "trade":
                query_res = self.trade_api.get_orders(
                    account_id=self.account_id,
                    order_status=OrderStatus.TRADED,
                    page=start_page,
                    limit=self.query_num,
                )
            else:
                query_res = self.trade_api.get_orders(
                    account_id=self.account_id,
                    page=start_page,
                    limit=self.query_num,
                )
            data_list = query_res["data"]
            for item in data_list:
                total_convert_list.append(self.convert_item(item, ftype))

            if len(query_res) == self.query_num:
                start_page += 1
            else:
                break

        df = pd.DataFrame(total_convert_list)
        dump_name = "{}_{}.csv".format(self.today_str, ftype)
        self.dump_to_csv(df, dump_name)

    def query_position(self):
        pos_ret = self.trade_api.get_positions(account_id=self.account_id, limit=False)
        positions = pos_ret["data"]
        position_list = []
        for position in positions:
            position_list.append(self.convert_position_item(position))
        df = pd.DataFrame(position_list)
        dump_name = "{}_pos.csv".format(self.today_str)
        self.dump_to_csv(df, dump_name)

    def query_account(self):
        cash_info = self.trade_api.get_cash(
            account_id=self.account_id
        )
        print(cash_info)

    def dump_to_csv(self, df, dump_name):
        tmp_name = dump_name + ".tmp"
        df.to_csv(tmp_name, index=False)
        sub.check_call("mv {} {}".format(tmp_name, dump_name), shell=True)

    def convert_item(self, trade, ftype):
        res = dict()
        res["AccountUid"] = self.account_id
        res["OrderId"] = self.trader.sys_id_local_id_map[trade["client_task_id"]]
        res["OrderSysId"] = trade["client_task_id"]
        eid_list = trade["wid"].split(".")
        res["InstrumentId"] = eid_list[0]
        res["Exchange"] = eid_list[1]
        res["OrderSide"] = "Buy" if trade["side"] == 1 else "Sell"
        res["FillSize"] = trade["traded_volume"]
        res["FillPrice"] = trade["avg_traded_price"]
        res["TradeStatus"] = self.convert_order_status(trade["order_status"])
        res["TradeTime"] = self.convert_order_time(trade["last_update_time"])
        return res

    def convert_position_item(self, position):
        res = dict()
        res["AccountUid"] = position["account_id"]
        eid_list = position["wid"].split(".")
        res["InstrumentId"] = eid_list[0]
        res["ExchangeId"] = eid_list[1]
        res["FinanceType"] = "Stock"
        res["InitYestSize"] = position["yd_long_pos"] + position["yd_short_pos"]
        res["TotalBuySize"] = position["td_long_pos"]
        res["TotalSellSize"] = position["td_short_pos"]
        return res

    def convert_order_status(self, status):
        if status == OrderStatus.INSERTED:
            return "Accepted"
        elif status == OrderStatus.INSERTFAILED:
            return "Rejected"
        elif status == OrderStatus.TRADED or status == OrderStatus.TRADEDPARTIAL:
            return "PartTraded"
        elif status == OrderStatus.CANCELLFAILED:
            return "CancelFailed"
        elif status == OrderStatus.CANCELLED:
            return "CancelDone"
        return "Unknown"

    def convert_order_time(self, origin_time):
        d = dt.datetime.fromtimestamp(origin_time / 1000000.0)
        return d.strftime("%Y%m%d%H%M%S%f")[:-3]

def test(config_file="./account.ini"):
    config = configparser.ConfigParser()
    config.read(config_file)
    q = HPIQuery(config["Trade"])
    #q.query_order();
    q.query("trade")
    q.query("sub_order")
    q.query_position();
    #q.query_account()

if __name__ == '__main__':
    argh.dispatch_commands([test])
