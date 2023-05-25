from HPI.wsclient import WSClient

class TraderCallback:
    def __init__(self, config, token, trader):
        self.config = config
        self.ws_client = WSClient(
            config["server_addr"],
            user=config["trader_user"],
            cli_token=token,
        )
        self.trader = trader

    def init(self):
        self.ws_client.regist("orders", self.OnRtnOrder)
        self.ws_client.regist("tasks", self.OnRtnTask)
        self.ws_client.regist("positions", self.OnQryPosition)
        self.ws_client.regist("account_info", self.OnQryAccount)

    def OnRtnOrder(self, sub_orders):
        self.trader.logger.info("OnRtnOrder, sub order")

    def OnRtnTask(self, ori_orders):
        self.trader.logger.info("OnRtnTask, origin order")

    def OnQryPosition(self, positions):
        #self.trader.logger.info("OnQryPosition")
        self.trader.logger.info("{}".format(len(positions)))

    def OnQryAccount(self, account_info):
        #self.trader.logger.info("OnQryAccount")
        self.trader.logger.info(account_info)

    def start(self):
        self.trader.logger.info("Websocket client Start")
        self.ws_client.start()
