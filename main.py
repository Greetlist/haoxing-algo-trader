import argh
import trader
import configparser

def start(config_file="./account.ini"):
    config = configparser.ConfigParser()
    config.read(config_file)
    hpi_trader = trader.HPITrader(config["Trade"])
    hpi_trader.start()

if __name__ == "__main__":
    argh.dispatch_commands([start])
