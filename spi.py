from HPI.wsclient import WSClient

class TraderCallback:
    def __init__(self, config, token):
        self.config = config
        self.ws_client = WSClient(
            config["server_addr"],
            user=config["user"],
            cli_token=token,
        )

    def init(self):
        self.ws_client.regist("orders", self.OnRtnOrder)
        self.ws_client.regist("tasks", self.OnRtnTask)
        self.ws_client.regist("positions", self.OnQryPosition)
        self.ws_client.regist("account_info", self.OnQryAccount)

    def OnRtnOrder(self, sub_orders):
        print("OnRtnOrder, sub order")

    def OnRtnTask(self, ori_orders):
        print("OnRtnTask, origin order")

    def OnQryPosition(self, positions):
        print("OnQryPosition")

    def OnQryAccount(self, account_info):
        print("OnQryAccount")
        print(account_info)

    def start(self):
        self.ws_client.start()
