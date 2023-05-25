from HPI import StockTask, HttpClient, AlgoType, TradeType, QtyType
from HPI.constants import OrderStatus
import argh
import configparser
import datetime as dt
import pandas as pd
import subprocess as sub

class HPIQuery:
    def __init__(self, config):
        self.config = config
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

    def query_trade(self):
        start_page = 1
        total_trade = []
        while True:
            trade_res = self.trade_api.get_orders(
                account_id=self.account_id,
                order_status=OrderStatus.TRADED,
                page=start_page,
                limit=self.query_num,
            )
            trade_list = trade_res["data"]
            for trade in trade_list:
                total_trade.append(self.convert_trade_item(trade))

            if len(trade_res) == self.query_num:
                start_page += 1
            else:
                break

        df = pd.DataFrame(total_trade)
        dump_csv = "{}_trade.csv".format(self.today_str)
        tmp_csv = dump_csv + ".tmp"
        df.to_csv(tmp_csv, index=False)
        sub.check_call("mv {} {}".format(tmp_csv, dump_csv), shell=True)

    def query_position(self):
        pos_ret = self.trade_api.get_positions(account_id=self.account_id)
        position_list = pos_ret["data"]
        print(pos_ret["total"])
        #for position in position_list:
        #    print('Position: {}'.format(position))

    def query_account(self):
        cash_info = self.trade_api.get_cash(
            account_id=self.account_id
        )
        print(cash_info)

    def convert_trade_item(self, trade):
        convert_item = dict()
        convert_item["AccountUid"] = self.account_id
        convert_item["OrderId"] = 0 #TODO
        convert_item["OrderSysId"] = trade["order_sysid"]
        eid_list = trade["wid"].split(".")
        convert_item["InstrumentId"] = eid_list[0]
        convert_item["Exchange"] = eid_list[1]
        convert_item["OrderSide"] = "Buy" if trade["side"] == 1 else "Sell"
        convert_item["FillSize"] = trade["traded_volume"]
        convert_item["FillPrice"] = trade["avg_traded_price"]
        convert_item["TradeStatus"] = self.convert_order_status(trade["order_status"])
        convert_item["TradeTime"] = self.convert_order_time(trade["last_update_time"])
        return convert_item

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
    q.query_trade()
    q.query_trade()
    #q.query_position();
    #q.query_account()

if __name__ == '__main__':
    argh.dispatch_commands([test])
