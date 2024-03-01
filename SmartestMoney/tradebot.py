import ccxt
from pprint import pprint
from dotenv import load_dotenv
import os

load_dotenv()

#https://testnet.binancefuture.com/zh/futures/BTCUSDT

api_key = os.getenv("TESTNET_API_KEY")
secret = os.getenv("TESTNET_SECRET")
# rsa_pri_path = os.getenv("TESTNET_SECRET")

# with open(rsa_pri_path, 'r') as file:
#     secret = file.read()

exchange_usdm = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,
    
})
exchange_usdm.set_sandbox_mode(True)

exchange_coinm = ccxt.binancecoinm({
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,
})
exchange_coinm.set_sandbox_mode(True)

def filter_symbols(symbols_info):
    filtered_symbols_info = [] 

    for item in symbols_info:
        symbol = item['symbol']
        entry_price = item['entryPrice']
        
        if symbol.endswith('USD_PERP'): # 币本位合约  
            symbol = symbol.replace('USD_PERP', '/USD')        
            exchange = exchange_coinm
            item['contract_type'] = 'coinm'
        else: # U本位合约           
            exchange = exchange_usdm
            item['contract_type'] = 'usdm'     
        try:
            ticker = exchange.fetchTicker(symbol)
            last_price = ticker['last']
            if last_price is None:  # 如果没有获取到最新价格，则跳过当前循环的剩余部分
                print(f"No price data for {symbol}")
                continue
        
            if abs(last_price - entry_price) / entry_price <= 0.5: # 判断价格是否在50%偏差以内
                item['symbol'] = symbol
                filtered_symbols_info.append(item)
        except ccxt.BaseError as e:
            print(f"Error fetching ticker for {symbol}: {e}")
            continue

    return filtered_symbols_info

symbols_info = [
  {
    "symbol": "BTCUSD_PERP",
    "entryPrice": 63000
  },
  {
    "symbol": "BLURUSDT",
    "entryPrice": 0.60234
  }
]

filtered_symbols_info = filter_symbols(symbols_info)
for item in filtered_symbols_info:
    print(item)

