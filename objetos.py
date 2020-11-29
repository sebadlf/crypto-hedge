import datetime as dt

##################################
# Definicion de la clase Ticker
class Ticker:

    def __init__(self, symbol):
        self.symbol = symbol
        self.description = ""
        self.price = []

    def add_price(self, broker, price):
        p = Price()
        p.time = dt.datetime.utcnow()
        p.broker = broker
        p.price = price

        self.price.append(p)

    def get_price(self):
        return f"${self.price[-1].price}"

    def download_price(self):
        pass

    def save_to_database(self):
        pass

##################################
# Definicion de la clase Ticker
class Price:
    time = ""
    broker = ""
    price = 0

    def __repr__(self):
        return f"{self.broker} {self.price}"

##################################

# Instancia BTC

btc = Ticker("BTC")
btc.description = "Some desc"

btc.add_price('okex', 16000)
btc.add_price('okex', 17000)
btc.add_price('okex', 17800)

##################################

# Instancia ETH

other = Ticker("ETH")
other.description = "Ethernum Desc"

other.add_price('binance', 505)
other.add_price('binance', 510)
other.add_price('binance', 505)

##################################

print(f"{btc.symbol} {btc.get_price()}")

print(f"{other.symbol} {other.get_price()}")
