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
        print(login_res)
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

        order_list = order_res["data"]
        for order in order_list:
            print(order)
        if len(order_res) == self.query_num:
            self.query_sub_order_num += 1

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
                print("Trade: {}".format(trade))

            total_trade.extend(trade_res)
            if len(trade_res) == self.query_num:
                start_page += 1
            else:
                break
        print(total_trade)

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

def test(config_file="./account.ini"):
    config = configparser.ConfigParser()
    config.read(config_file)
    q = HPIQuery(config["Trade"])
    #q.query_order();
    #q.query_trade();
    q.query_position();
    q.query_account()

if __name__ == '__main__':
    argh.dispatch_commands([test])
