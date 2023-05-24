from HPI import StockTask, HttpClient, AlgoType, TradeType, QtyType
from HPI.constants import OrderStatus
import argh
import configparser
import datetime as dt

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
        self.account_id = config["fund_account"]
        self.query_num = config["query_num"]

        self.query_sub_order_page = 1
        self.query_ori_order_page = 1

    def query_origin_order(self):
        origin_orders = self.trade_api.get_tasks(
            account_id=self.account_id,
            page=self.query_ori_order_page,
            limit=self.query_num,
        )
        if len(origin_orders) == self.query_num:
            self.query_ori_order_page += 1

        for order in origin_orders:
            print(order)

    def query_order(self):
        order_res = self.trade_api.get_orders(
            account_id=self.account_id, 
            page=self.query_sub_order_page,
            limit=self.query_num,
        )
        if len(order_res) == self.query_num:
            self.query_sub_order_num += 1

        for order in sub_orders:
            print(order)

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
            for trade in trade_res:
                print(trade)

            total_trade.extend(trade_res)
            if len(trade_res) == self.query_num:
                start_page += 1
            else:
                break
        print(total_trade)

    def query_position(self):
        positions = self.trade_api.get_positions(account_id=self.account_id)
        for position in positions:
            print(position)

    def query_account(self):
        cash_info = self.trade_api.get_cash(
            account_id=self.account_id
        )
        print(cash_info)

def test(config_file="./account.ini"):
    config = configparser.ConfigParser()
    config.read(config_file)
    q = HPIQuery(config["Trade"])
    q.query_order();
    q.query_trade();
    q.query_position();
    q.query_account()

if __name__ == '__main__':
    argh.dispatch_commands([test])
