import json
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func
from datetime import datetime
import time
from dotenv import load_dotenv
import os
import logging
import httpx
from pydantic import BaseModel, ValidationError
from typing import List
import re
from apscheduler.schedulers.background import BackgroundScheduler
import ccxt.pro as ccxtpro
import pandas as pd
import ccxt
import asyncio

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Base = declarative_base()
engine = create_engine('sqlite:///hzans.db')
Session = sessionmaker(bind=engine)

api_key = os.getenv("API_KEY")
rsa_pri_path = os.getenv("RSA_PRI")

with open(rsa_pri_path, 'r') as file:
    secret = file.read()

exchange = ccxt.binanceusdm({
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,  
})

exchange_usdm = ccxtpro.binanceusdm({
    'apiKey': api_key,
    'secret': secret,
    'enableRateLimit': True,  
})

INITIAL_CAPITAL = 30000
MAX_LEVERAGE = 8

class Position(Base):
    __tablename__ = 'positions'

    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    entryPrice = Column(Float)
    amount = Column(Float)
    leverage = Column(Integer)
    markPrice = Column(Float) 
    pnl = Column(Float)
    roe = Column(Float)
    updateTime = Column(String)
    updateTimeStamp = Column(Integer, unique=True)
    yellow = Column(Boolean)
    tradeBefore = Column(Boolean)
    avgPrice = Column(Float)
    stakeAmount = Column(Float)
    capitalPercent = Column(Float)
    source = Column(String)

class Capital(Base):
    __tablename__ = 'capital'

    id = Column(Integer, primary_key=True)
    weeklyRoi = Column(Float)
    weeklyPnl = Column(Float)
    currentCapital = Column(Float)
    updateTime = Column(DateTime)
    updateTimeStamp = Column(Integer, unique=True)

class MyPosition(Base):
    __tablename__ = 'my_positions'

    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    entryDate = Column(DateTime)
    entryPrice = Column(Float)
    amount = Column(Float)
    leverage = Column(Integer)
    updateTime = Column(String)
    updateTimeStamp = Column(Integer, unique=True)
    stakeAmount = Column(Float)
    capitalPercent = Column(Float)
    exitDate = Column(DateTime)
    exitPrice = Column(Float)
    exitAmount = Column(Float)
    PnL = Column(Float)  
    PnLRatio = Column(Float)
    singleTradeROI = Column(Float)
    totalCapital = Column(Float)

def init_db():
    Base.metadata.create_all(engine)

def retrieve_positions(tradeType):
    '''获取排行榜里的仓位信息'''
    try:
        url = 'https://binance-futures-leaderboard1.p.rapidapi.com/v1/getOtherPosition'
        querystring = {'encryptedUid':'ACD6F840DE4A5C87C77FB7A49892BB35', 'tradeType':tradeType} #PERPETUAL 是 U 本位，DELIVERY是币本位
        headers = {
            'X-RapidAPI-Key': '9cb3faa370msh6ea8f37e65e4111p162897jsn1e9c3cc8f936',
            'X-RapidAPI-Host': 'binance-futures-leaderboard1.p.rapidapi.com'
        }
        transport = httpx.HTTPTransport(retries=5)
        client = httpx.Client(transport=transport)
        response = client.get(url, headers=headers, params=querystring)

        if response.status_code == 200:
            data = response.json().get('data', {})
            positions_list = data.get('otherPositionRetList', [])

            for position in positions_list:
                position['updateTime'] = datetime(*position['updateTime'][:6]).strftime('%Y-%m-%d %H:%M:%S')
            return positions_list
        else:
            logging.warning(f"Failed to retrieve positions, status code: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f'Error retrieving positions: {e}')
        return []

def retrieve_performance():
    '''获取WEEKLY表现数据中的ROI和PNL值'''
    url = "https://binance-futures-leaderboard1.p.rapidapi.com/v1/getOtherPerformance"
    querystring = {"encryptedUid":"ACD6F840DE4A5C87C77FB7A49892BB35","tradeType":"PERPETUAL"}
    headers = {
        "X-RapidAPI-Key": "9cb3faa370msh6ea8f37e65e4111p162897jsn1e9c3cc8f936",
        "X-RapidAPI-Host": "binance-futures-leaderboard1.p.rapidapi.com"
    }
    transport = httpx.HTTPTransport(retries=5)
    client = httpx.Client(transport=transport)

    try:
        response = client.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raises exception for 4XX or 5XX errors
    except httpx.HTTPStatusError as e:
        logging.error(f'HTTP error occurred: {e}')
        return None
    except Exception as e:
        logging.error(f'Error retrieving performance: {e}')
        return None
    
    data = response.json().get('data', [])
    performance = {item['statisticsType']: item['value'] for item in data if item['periodType'] == 'WEEKLY'}
    weekly_roi = performance.get('ROI')
    weekly_pnl = performance.get('PNL')

    if weekly_roi is not None and weekly_pnl is not None:
        session = Session()
        if weekly_roi != 0: # 计算当前本金
            current_capital = weekly_pnl / weekly_roi + weekly_pnl
        else:
            current_capital = weekly_pnl

        capital_record = session.query(Capital).first() # 检查数据库中是否有记录
        if capital_record:
            capital_record.weeklyRoi = weekly_roi
            capital_record.weeklyPnl = weekly_pnl
            capital_record.currentCapital = current_capital
            capital_record.updateTime = datetime.utcnow()
            capital_record.updateTimeStamp = int(time.time())
        else:
            new_record = Capital(weeklyRoi=weekly_roi, weeklyPnl=weekly_pnl, currentCapital=current_capital, updateTime=datetime.utcnow(), updateTimeStamp=int(time.time()))
            session.add(new_record)

        try:
            session.commit()
            logging.info(f"💰 Capital updated {current_capital}")
        except Exception as e:
            logging.error(f"Error while inserting capital: {e}")
            session.rollback()
        finally:
            session.close()

        return current_capital
    else:
        logging.warning("WEEKLY ROI or PNL data not found")
        return None

def fetch_eth_price():
    try:
        ticker = exchange.fetchTicker('ETH/USDT')
        return ticker['last']
    except ccxt.BaseError as e:
        logging.error(f"Error fetching ticker for ETH/USDT: {e}")

def fetch_symbol_info():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    transport = httpx.HTTPTransport(retries=5)
    client = httpx.Client(transport=transport)
    try:
        response = client.get(url)
        response.raise_for_status()  # Raises exception for 4XX or 5XX errors
    except httpx.HTTPStatusError as e:
        logging.error(f'HTTP error occurred: {e}')
        return None
    except Exception as e:
        logging.error(f'Error retrieving performance: {e}')
        return None    
    data = response.json()   
    file_path = os.path.join("BINANCE", "symbol_info.json")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4) 
    return data

def calculate_positions():
    session = Session()
    # 获取currentCapital
    capital_record = session.query(Capital.currentCapital).first()
    currentCapital = capital_record.currentCapital if capital_record else None
    
    if currentCapital is None:
        logging.error("No current capital found.")
        return
    
    # 从positions表获取数据
    positions = session.query(Position).all()
    
    # 转换数据到DataFrame
    position_df = pd.DataFrame([{
        'symbol': position.symbol,
        'entryPrice': position.entryPrice,
        'markPrice': position.markPrice,
        'pnl': position.pnl,
        'amount': position.amount,
        'leverage': position.leverage
    } for position in positions])

    # 计算新列
    def calculate_row(row):
        if row['symbol'].endswith('USDT'):
            avg_price = row['markPrice'] - row['pnl'] / row['amount']
            stake_amount = row['amount'] * avg_price / row['leverage']
        elif row['symbol'] == 'ETHBTC':
            avg_price = row['entryPrice']
            eth_price = fetch_eth_price() 
            stake_amount = row['amount'] * eth_price / row['leverage']
        else:
            return row  # 对于其他symbol，不进行计算
        
        capital_percent = stake_amount / currentCapital if currentCapital else 0
        row['avgPrice'] = avg_price
        row['stakeAmount'] = stake_amount
        row['capitalPercent'] = capital_percent
        return row

    position_df = position_df.apply(calculate_row, axis=1)

    for index, row in position_df.iterrows(): #更新到数据库
        position = session.query(Position).filter(Position.symbol == row['symbol']).first()
        if position:
            position.avgPrice = row['avgPrice'] if 'avgPrice' in row else position.avgPrice
            position.stakeAmount = row['stakeAmount'] if 'stakeAmount' in row else position.stakeAmount
            position.capitalPercent = row['capitalPercent'] if 'capitalPercent' in row else position.capitalPercent

            try:
                session.commit()
                logging.info(f"Updated {position.symbol}")
            except Exception as e:
                session.rollback()
                logging.error(f"Error updating {position.symbol}: {e}")
        else:
            logging.error(f"No position found for {row['symbol']}")

    session.close()
    return position_df

def get_symbol_info(symbols, position_df):
    symbols_info = []
    with open('BINANCE/symbol_info.json', 'r', encoding='utf-8') as file:
        data = json.load(file)    

    for symbol in symbols:
        if symbol in position_df['symbol'].values:
            symbol_row = position_df[position_df['symbol'] == symbol].iloc[0]
            capital_percent = symbol_row['capitalPercent']
            leverage = symbol_row['leverage']
            stake_amount = INITIAL_CAPITAL * capital_percent
            leverage = min(leverage, MAX_LEVERAGE)
            stepSize = next(
                (f["stepSize"] for s in data["symbols"] if s["symbol"] == symbol for f in s["filters"] if f["filterType"] == 'LOT_SIZE'),
                None)

            if stepSize is None:
                fetched_data = fetch_symbol_info()  # 确保这个函数能返回有效的数据结构，与本地JSON相同
                stepSize = next(
                    (f["stepSize"] for s in fetched_data["symbols"] if s["symbol"] == symbol for f in s["filters"] if f["filterType"] == 'LOT_SIZE'),
                    '1')

            if '.' in str(stepSize):
                decimal_places = len(stepSize.split('.')[1])
            else:
                decimal_places = 0 

            symbols_info.append({
                "symbol": symbol,
                "capitalPercent": capital_percent,
                "leverage": leverage,
                "stakeAmount": stake_amount,
                "decimalPlaces": decimal_places
            })
        else:
            logging.error(f"Symbol {symbol} not found in the provided DataFrame.")
        
    return symbols_info

def get_latest_updateTimeStamp(symbol):
    '''获取数据库里最新时间戳'''
    session = Session()
    try:
        result = session.query(Position.updateTimeStamp)\
                        .filter(Position.symbol == symbol)\
                        .order_by(Position.updateTimeStamp.desc())\
                        .first()
        return result.updateTimeStamp if result else None
    finally:
        session.close()

def get_all_symbols_and_latest_timestamps():
    '''获取数据库里仓位的最新 symbol 和时间戳'''
    session = Session()
    try:
        all_positions = session.query(
            Position.symbol, 
            Position.updateTimeStamp
        ).order_by(Position.symbol, Position.updateTimeStamp.desc()).all()
        
        symbols_timestamps = {}
        for symbol, timestamp in all_positions:
            if symbol not in symbols_timestamps:
                symbols_timestamps[symbol] = timestamp

        return symbols_timestamps
    finally:
        session.close()

def initial_insert_positions():
    perpetual_positions = retrieve_positions(tradeType='PERPETUAL')
    delivery_positions = retrieve_positions(tradeType='DELIVERY')
    all_positions = perpetual_positions + delivery_positions
    
    session = Session()
    
    for item in all_positions:
        position = Position(
            symbol=item['symbol'], 
            entryPrice=item['entryPrice'], 
            markPrice=item['markPrice'], 
            pnl=item['pnl'], 
            roe=item['roe'], 
            updateTime=item['updateTime'],
            amount=item['amount'], 
            updateTimeStamp=item['updateTimeStamp'],
            yellow=item['yellow'], 
            tradeBefore=item['tradeBefore'], 
            leverage=item['leverage'],
            source='leaderboard'
        )
        session.add(position)
    
    try:
        # 提交到数据库
        session.commit()
    except Exception as e:
        logging.error(f"Error while inserting positions: {e}")
        session.rollback()
    finally:
        session.close()

def add_new_trades(new_symbols, data, source):
    session = Session()
    try:
        for item in data:
            if item['symbol'] in new_symbols:
                new_position = Position(
                    symbol=item['symbol'],
                    entryPrice=item['entryPrice'],
                    markPrice=item.get('markPrice', None),
                    pnl=item.get('pnl', None),
                    roe=item.get('roe', None),
                    updateTime=item['updateTime'],
                    amount=item['amount'],
                    updateTimeStamp=item['updateTimeStamp'],
                    yellow=item.get('yellow', None),
                    tradeBefore=item.get('tradeBefore', None),
                    leverage=item['leverage'],
                    source=source
                )
                session.add(new_position)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error adding new trades: {e}")
    finally:
        session.close()

def close_existing_trades(closed_symbols):
    session = Session()
    try:
        for symbol in closed_symbols:
            # 查询所有匹配的记录并删除
            session.query(Position).filter(Position.symbol == symbol).delete()
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error closing existing trades: {e}")
    finally:
        session.close()

def update_existing_trades(data, updated_symbols):
    session = Session()
    try:
        for symbol in updated_symbols:  # 遍历需要更新的symbols
            item = next((item for item in data if item['symbol'] == symbol), None)
            if not item:
                continue  # 如果在api_data中找不到这个symbol的记录，跳过

            current_position = session.query(Position).filter(Position.symbol == symbol).first()
            if not current_position:
                continue  # 如果数据库中没有这个symbol的记录，跳过

            # 检查amount的变化
            if item['amount'] > current_position.amount:
                logging.info(f"⬆️ Increased a position for {symbol}")
                current_position.amount = item['amount']
            elif item['amount'] < current_position.amount:
                logging.info(f"⬇️ Decreased a position for {symbol}")
                current_position.amount = item['amount']

            # 检查leverage的变化
            if 'leverage' in item and item['leverage'] > current_position.leverage:
                logging.info(f"🔺 Increased leverage for {symbol}")
                current_position.leverage = item['leverage']
            elif 'leverage' in item and item['leverage'] < current_position.leverage:
                logging.info(f"🔻 Decreased leverage for {symbol}")
                current_position.leverage = item['leverage']

            # 更新其他字段
            current_position.entryPrice = item['entryPrice']
            current_position.markPrice = item.get('markPrice', current_position.markPrice)
            current_position.pnl = item.get('pnl', current_position.pnl)
            current_position.roe = item.get('roe', current_position.roe)
            current_position.updateTime = item['updateTime']
            current_position.updateTimeStamp = item['updateTimeStamp']
            current_position.yellow = item.get('yellow', current_position.yellow)
            current_position.tradeBefore = item.get('tradeBefore', current_position.tradeBefore)

        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error updating existing trades: {e}")
    finally:
        session.close()

def order_to_database(order_status, symbol, leverage, stakeAmount, capitalPercent):
    session = Session()
    if float(order_status['filled']) == 0:  # 跳过未成交订单
        return
    
    existing_position = session.query(MyPosition).filter_by(symbol=symbol).first() # 检查数据库中是否已经存在这个symbol

    if existing_position is None:
        entryPrice = float(order_status['average'])
        amount = float(order_status['filled'])
    else:
        amount = existing_position.amount + order_status['filled']
        entryPrice = (existing_position.amount * existing_position.entryPrice + 
                      float(order_status['average']) * float(order_status['filled'])) / (existing_position.amount + float(order_status['filled']))
        stakeAmount += existing_position.stakeAmount
        capitalPercent += existing_position.capitalPercent

    totalCapital_row = session.query(MyPosition).order_by(MyPosition.updateTimeStamp.desc()).first()
    totalCapital = totalCapital_row.totalCapital if totalCapital_row else INITIAL_CAPITAL

    entryDate = datetime.strptime(order_status['datetime'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")

    position_data = {
        'symbol': symbol,
        'entryDate': entryDate,
        'entryPrice': entryPrice,
        'amount': amount,
        'leverage': leverage,
        'updateTime': entryDate,
        'updateTimeStamp': order_status['timestamp'],
        'stakeAmount': stakeAmount,
        'capitalPercent': capitalPercent,
        'totalCapital': totalCapital
    }

    try:
        if existing_position:
            for key, value in position_data.items():
                setattr(existing_position, key, value)
            session.merge(existing_position)
        else:
            new_position = MyPosition(**position_data)
            session.add(new_position)
        session.commit()    

    except Exception as e:
        session.rollback()
        logging.error(f"Error during save order {order_status} to db: {e}")
    finally:
        session.close()

async def watch_order_book(symbol, queue, exchange_usdm):
    while True:
        try:
            orderbook = await exchange_usdm.watch_order_book(symbol)
            buy_price = orderbook['bids'][0][0]
            await queue.put(buy_price)
        except ccxt.BaseError as e:
            logging.error(f"Error opening position for {symbol}: {e}")

async def open_positions(symbols_info, queue, exchange_usdm):
    for item in symbols_info:
        symbol = item['symbol']
        stakeAmount = float(item['stakeAmount']) 
        leverage = int(item['leverage'])
        decimalPlaces = int(item['decimalPlaces'])
        capitalPercent = float(item['capitalPercent'])
                
        try:           
            buy_price = await queue.get()     
            amount = stakeAmount * leverage / buy_price
            adjusted_amount = round(amount, decimalPlaces)
            if decimalPlaces == 0:
                adjusted_amount = int(adjusted_amount)
            balance = await exchange_usdm.fetch_balance()

            available_balance = float(balance['info']['availableBalance'])
            if available_balance < stakeAmount:
                logging.error(f"U 本位合约余额不足，无法下单: {symbol}")
                continue

            setleverage = await exchange_usdm.set_leverage(leverage, symbol) 
            logging.info(setleverage)
            setmargin = await exchange_usdm.setMarginMode('cross', symbol)
            logging.info(setmargin)
            order = await exchange_usdm.createOrder(symbol, 'limit', 'buy', adjusted_amount, buy_price)  
            order_to_database(order, symbol, leverage, stakeAmount, capitalPercent)

            await asyncio.wait_for(check_order_filled(exchange_usdm, order, symbol, buy_price, leverage, stakeAmount, capitalPercent), timeout=600)               

        except ccxt.BaseError as e:
            logging.error(f"Error opening position for {symbol}: {e}")
            continue
        
        except asyncio.TimeoutError:
            canceled = await exchange_usdm.cancelOrder(order['id'], symbol)
            logging.info(f"Unfilled order canceled: {canceled}")
            order_status = await exchange_usdm.fetchOrder(order['id'], symbol)
            remaining_amount = order_status['remaining']
            market_order = await exchange_usdm.createOrder(symbol, 'market', 'buy', remaining_amount)  
            logging.info(f"Market order placed for the remaining amount: {market_order}")
            order_to_database(market_order, symbol, leverage, stakeAmount, capitalPercent)

async def check_order_filled(exchange_usdm, order, symbol, buy_price, leverage, stakeAmount, capitalPercent):
    while True:         
        order_status = await exchange_usdm.fetchOrder(order['id'], symbol)
        remaining_amount = order_status['remaining']
        logging.info(f"Order execution status: {order_status}")

        if remaining_amount > 0:                                        
            logging.info(buy_price)
            logging.info(f"Modifying order {order['id']}: Remaining amount {remaining_amount}, New buy price {buy_price}")
            edited_order = await exchange_usdm.editOrder(order['id'], symbol, 'limit', 'buy', remaining_amount, buy_price)
            logging.info(f"Order modified: {edited_order}")
            await asyncio.sleep(2)  # 等待2秒再次检查
        else:
            logging.info("Order fully filled.")
            order_status = await exchange_usdm.fetchOrder(order['id'], symbol)
            order_to_database(order_status, symbol, leverage, stakeAmount, capitalPercent)
            break  # 订单完全成交，退出循环 

def process_symbols(data, source):
    if not data:
        return
    db_symbols_timestamps = get_all_symbols_and_latest_timestamps()
    new_symbols_timestamps = {item['symbol']: item['updateTimeStamp'] for item in data}
    
    # 比较数据库和API/tweet的symbol，确定新增和减少的symbol
    new_symbols = set(new_symbols_timestamps.keys()) - set(db_symbols_timestamps.keys())
    closed_symbols = set(db_symbols_timestamps.keys()) - set(new_symbols_timestamps.keys())
    
    # 确定需要更新的symbols
    updated_symbols = {symbol for symbol, timestamp in new_symbols_timestamps.items()
                       if symbol in db_symbols_timestamps and timestamp > db_symbols_timestamps[symbol]}
    
    # 根据情况调用相应函数
    if new_symbols:
        add_new_trades(new_symbols, data, source=source)
    if closed_symbols:
        close_existing_trades(closed_symbols)
    if updated_symbols:
        update_existing_trades(updated_symbols, data)

def update_trade():
    perpetual_positions = retrieve_positions(tradeType='PERPETUAL')
    delivery_positions = retrieve_positions(tradeType='DELIVERY')
    api_data = perpetual_positions + delivery_positions
    process_symbols(api_data, 'leaderboard')

# init_db()
# initial_insert_positions()
# res = retrieve_performance()
# print(res)
# df_result = calculate_positions() #可以每当仓位更新时调一次
# print(df_result)
    
# res = fetch_symbol_info()
# print(res)
    
# async def main():
#     exchange_usdm = ccxtpro.binanceusdm({
#         'apiKey': api_key,
#         'secret': secret,
#         'enableRateLimit': True,  
#     })
#     queue = asyncio.Queue()        
#     watcher = watch_order_book(symbol[0]["symbol"], queue, exchange_usdm)   
#     consumer = open_positions(symbol, queue, exchange_usdm)
#     try:
#         await asyncio.gather(watcher, consumer)
#     finally:
#         await exchange_usdm.close()

# if __name__ == '__main__':
#     asyncio.run(main())


# scheduler = BackgroundScheduler()
# scheduler.add_job(func=update_trade, trigger='interval', minutes=1, next_run_time=datetime.now())
# scheduler.start()

# # 保持主线程运行，防止程序结束
# try:
#     while True:
#         time.sleep(1)
# except (KeyboardInterrupt, SystemExit):
#     scheduler.shutdown()