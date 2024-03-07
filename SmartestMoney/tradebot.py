from pprint import pprint
from dotenv import load_dotenv
import os
import ccxt.pro as ccxtpro
import asyncio
import ccxt
import logging

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

load_dotenv()

#https://testnet.binancefuture.com/zh/futures/BTCUSDT

api_key = os.getenv("TESTNET_API_KEY")
secret = os.getenv("TESTNET_SECRET")
# rsa_pri_path = os.getenv("TESTNET_SECRET")

# with open(rsa_pri_path, 'r') as file:
#     secret = file.read()

exchange_usdm = ccxtpro.binanceusdm({
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,
    
})
exchange_usdm.set_sandbox_mode(True)

exchange_coinm = ccxtpro.binancecoinm({
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,
})
exchange_coinm.set_sandbox_mode(True)

async def filter_symbols(symbols_info):
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
            ticker = await exchange.fetchTicker(symbol)
            last_price = ticker['last']
            if last_price is None:  # 如果没有获取到最新价格，则跳过当前循环的剩余部分
                print(f"No price data for {symbol}")
                continue
        
            if abs(last_price - entry_price) / entry_price <= 0.5: # 判断价格是否在50%偏差以内
                item['symbol'] = symbol
                filtered_symbols_info.append(item)
            await exchange.close()

        except ccxtpro.BaseError as e:
            print(f"Error fetching ticker for {symbol}: {e}")
            continue

    return filtered_symbols_info

async def open_positions(symbols_info):
    for item in symbols_info:
        symbol = item['symbol']
        contract_type = item['contract_type']
        amount = float(item['amount'])
        leverage = int(item['leverage'])

        if contract_type == 'coinm':
            exchange = exchange_coinm
        elif contract_type == 'usdm':
            exchange = exchange_usdm
        try:           
            orderbook = await exchange.watch_order_book(symbol)
            buy_price = orderbook['bids'][0][0]  # 获取买一价     
            balance = await exchange.fetch_balance()

            if contract_type == 'usdm':
                available_balance = float(balance['info']['availableBalance'])
                if available_balance < amount * buy_price / leverage:
                    logger.error(f"U 本位合约余额不足，无法下单: {symbol}")
                    continue

            elif contract_type == 'coinm':
                # 从 symbol 中提取币名
                coin_name = symbol.split("/")[0]
                coin_available_balance = 0
                for asset_info in balance['info']['assets']:
                    if asset_info['asset'] == coin_name:
                        coin_available_balance = float(asset_info['availableBalance'])
                        break
                if coin_available_balance < amount / (buy_price / 10) / leverage:
                    logger.error(f"{coin_name} 币本位合约余额不足，无法下单: {symbol}")
                    continue    

            setleverage = await exchange.set_leverage(leverage, symbol) 
            print(setleverage)
            setmargin = await exchange.setMarginMode('cross', symbol)
            print(setmargin)
            order = await exchange.createOrder(symbol, 'limit', 'buy', amount, buy_price)  
            while True:             
                # order_status = await exchange.watch_orders(symbol) #不知为什么websocket版的 api 打不出结果
                order_status = await exchange.fetchOrder(order['id'], symbol)
                remaining_amount = order_status['remaining']
                print(f"Order execution status: {order_status}")
                if remaining_amount > 0:                                        
                    # 获取此时新的买一价
                    # orderbook = await exchange.watch_order_book(symbol) #这里不需要重新调用了吗？加上反而会卡住ws
                    new_buy_price = orderbook['bids'][0][0]
                    print(new_buy_price)
                    # 尝试修改订单
                    print(f"Modifying order {order['id']}: Remaining amount {remaining_amount}, New buy price {new_buy_price}")
                    edited_order = await exchange.editOrder(order['id'], symbol, 'limit', 'buy', remaining_amount, new_buy_price)
                    print(f"Order modified: {edited_order}")
                    await asyncio.sleep(2)  # 等待2秒再次检查
                else:
                    print("Order fully filled.")
                    break  # 订单完全成交，退出循环                

        except ccxt.BaseError as e:
            print(f"Error fetching order book for {symbol}: {e}")
            continue
        finally:
            await exchange.close()  
# async def main():
#     symbols_info = [
#     {
#         "symbol": "BTCUSD_PERP",
#         "entryPrice": 63000,
#         "amount": 47.6,
#         "leverage": 2,
#         "updateTime": "2024-02-01 08:52:42"
#     },
#     {
#         "symbol": "BLURUSDT",
#         "entryPrice": 0.60234,
#         "amount": 4997363,
#         "leverage": 2,
#         "updateTime": "2024-02-24 08:52:42"
#     }
#     ]

#     filtered_symbols_info = await filter_symbols(symbols_info)
#     for item in filtered_symbols_info:
#         print(item)

# if __name__ == "__main__":
#     asyncio.run(main())

# async def main():

#     while True:
#         orderbook = await exchange_usdm.watch_order_book('BTC/USDT')
#         print(orderbook['asks'][0], orderbook['bids'][0])
#         await exchange_usdm.close()
    
# asyncio.run(main())

symbols_info = [
{
    "symbol": "BTCUSD_PERP",
    "entryPrice": 63000,
    "amount": 0.05,
    "leverage": 2,
    "updateTime": "2024-02-01 08:52:42",
    'contract_type': 'coinm'
},
{
    "symbol": "ETCUSDT",
    "entryPrice": 37,
    "amount": 100,
    "leverage": 2,
    "updateTime": "2024-02-24 08:52:42",
    'contract_type': 'usdm'
}
]
asyncio.run(open_positions(symbols_info))