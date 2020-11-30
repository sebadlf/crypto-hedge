from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

TICKERS = ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']
FIRST_ROW_DATE = datetime.utcnow() - relativedelta(months=6)
