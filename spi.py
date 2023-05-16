from HPI.wsclient import  WSClient

class TraderCallback:
    def __init__(self, config):
        self.config = config
        self.ws_client = WSClient(
            config["server_addr"],
            user=config["user"],
            cli_token=config["cli_token"],
            timeout=config["ws_timeout"],
            interval=config["interval"],
        )

    def init(self):
        self.ws_client.regist("orders", self.OnRtnOrder)
        self.ws_client.regist("tasks", self.OnRtnTrade)
        self.ws_client.regist("positions", self.OnQryPosition)
        self.ws_client.regist("account_info", self.OnQryAccount)

    def OnRtnOrder(self):
        print("OnRtnOrder")

    def OnRtnTrade(self):
        print("OnRtnTrade")

    def OnQryPosition(self):
        print("OnQryPosition")

    def OnQryAccount(self):
        print("OnQryAccount")

    def start(self):
        self.ws_client.start()
