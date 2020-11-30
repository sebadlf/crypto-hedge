from binance import guardado_historico as guardado_historico_binance
from okex import guardado_historico as guardado_historico_okex
from config import TICKERS

while True:
    for ticker in TICKERS:
        print(ticker)
        guardado_historico_okex(ticker)
        guardado_historico_binance(ticker)