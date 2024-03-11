import json
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
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

def get_symbols_percent(symbol):
    session = Session()  
    symbol_percent = []

    try:
        current_position = session.query(Position).filter(Position.symbol == symbol).first()

        if current_position is not None:
            symbol_info = {
                'symbol': symbol,
                'before_capital_percent': current_position.capitalPercent
            }
            symbol_percent.append(symbol_info)
        else:
            logging.info(f"No position found for {symbol}")
            symbol_percent.append({'symbol': symbol, 'before_capital_percent': 0})
    except Exception as e:
        logging.error(f"Error while fetching symbols info: {e}")
    finally:
        session.close()  # 确保在结束时关闭数据库会话

    return symbol_percent

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

def get_symbol_info(symbols_info, position_df):
    session = Session()
    updated_symbols_info = []
    with open('BINANCE/symbol_info.json', 'r', encoding='utf-8') as file:
        data = json.load(file)    

    for symbol_info in symbols_info:
        symbol = symbol_info['symbol']
        before_capital_percent = symbol_info['before_capital_percent']

        if symbol in position_df['symbol'].values:
            symbol_row = position_df[position_df['symbol'] == symbol].iloc[0]
            capital_percent = symbol_row['capitalPercent'] - before_capital_percent
            leverage = symbol_row['leverage']
            totalCapital_row = session.query(MyPosition).order_by(MyPosition.updateTimeStamp.desc()).first()
            totalCapital = totalCapital_row.totalCapital if totalCapital_row else INITIAL_CAPITAL
            stake_amount = totalCapital * capital_percent 
            leverage = min(leverage, MAX_LEVERAGE)
            stepSize = next(
                (f["stepSize"] for s in data["symbols"] if s["symbol"] == symbol for f in s["filters"] if f["filterType"] == 'LOT_SIZE'),
                None)

            if stepSize is None:
                fetched_data = fetch_symbol_info()  
                stepSize = next(
                    (f["stepSize"] for s in fetched_data["symbols"] if s["symbol"] == symbol for f in s["filters"] if f["filterType"] == 'LOT_SIZE'),
                    '1')

            if '.' in str(stepSize):
                decimal_places = len(stepSize.split('.')[1])
            else:
                decimal_places = 0 

            updated_symbols_info.append({
                "symbol": symbol,
                "capitalPercent": capital_percent,
                "leverage": leverage,
                "stakeAmount": stake_amount,
                "decimalPlaces": decimal_places
            })
        else:
            logging.error(f"Symbol {symbol} not found in the provided DataFrame.")
        
    return updated_symbols_info

def fetch_amounts_for_symbols(symbols):
    session = Session()
    symbols_info = []
    
    for symbol in symbols:
        query_result = session.query(MyPosition).filter(MyPosition.symbol == symbol).first()
        
        if query_result:
            symbols_info.append({
                "symbol": symbol,
                "amount": query_result.amount
            })
        else:
            symbols_info.append({
                "symbol": symbol,
                "amount": None
            })

    session.close()
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
        logging.error(f"Error adding new trades{new_symbols}: {e}")
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
        logging.error(f"Error closing existing trades{closed_symbols}: {e}")
    finally:
        session.close()

def add_position(symbol, item):
    session = Session()
    logging.info(f"⬆️ Increasing position for {symbol}")
    current_position = session.query(Position).filter(Position.symbol == symbol).first()
    if current_position:
        current_position.amount = item['amount']
        current_position.updateTime = item['updateTime']
        current_position.updateTimeStamp = item['updateTimeStamp']
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error adding position {symbol}: {e}")
    finally:
        session.close()

def reduce_position(symbol, item):
    session = Session()
    logging.info(f"⬇️ Decreasing position for {symbol}")
    current_position = session.query(Position).filter(Position.symbol == symbol).first()
    if current_position:
        current_position.amount = item['amount']
        current_position.updateTime = item['updateTime']
        current_position.updateTimeStamp = item['updateTimeStamp']
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error reducing position {symbol}: {e}")
    finally:
        session.close()

def change_leverage(symbol, item):
    session = Session()
    if 'leverage' in item and item['leverage'] > current_position.leverage:
        logging.info(f"🔺 Increased leverage for {symbol}")

    elif 'leverage' in item and item['leverage'] < current_position.leverage:
        logging.info(f"🔻 Decreased leverage for {symbol}")

    current_position = session.query(Position).filter(Position.symbol == symbol).first()
    if current_position:
        current_position.leverage = item['leverage']
        current_position.updateTime = item['updateTime']
        current_position.updateTimeStamp = item['updateTimeStamp']
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error changing leverage {symbol}: {e}")
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

def update_sync(data):
    session = Session()
    for item in data:
        symbol = item['symbol']

        current_position = session.query(Position).filter_by(symbol=symbol).first()
        if not current_position:
            continue  # 如果数据库中没有这个symbol的记录，跳过    
        current_position.markPrice = item.get('markPrice', current_position.markPrice)
        current_position.pnl = item.get('pnl', current_position.pnl)
        current_position.roe = item.get('roe', current_position.roe)        
    try:
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Error updating markPrice and pnl: {e}")
    finally:
        session.close()

def order_to_database(order_status, symbol, leverage, stakeAmount, capitalPercent):
    session = Session()
    if float(order_status['filled']) == 0:  # 跳过未成交订单
        session.close()
        return
    
    existing_position = session.query(MyPosition).filter_by(symbol=symbol).first() # 检查数据库中是否已经存在这个symbol

    if existing_position is None:
        entryPrice = float(order_status['average'])
        amount = float(order_status['filled'])
    else:
        amount = existing_position.amount + float(order_status['filled'])
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

def close_order_to_database(order_status, symbol):
    session = Session()
    if float(order_status['filled']) == 0:
        session.close()
        return
    
    existing_position = session.query(MyPosition).filter_by(symbol=symbol).first()
    totalCapital_row = session.query(MyPosition).order_by(MyPosition.updateTimeStamp.desc()).first()
    totalCapital = totalCapital_row.totalCapital if totalCapital_row else INITIAL_CAPITAL
    
    if not existing_position.exitAmount:
        exitPrice = float(order_status['average'])
        exitAmount = float(order_status['filled'])
    else:
        exitAmount = existing_position.exitAmount + float(order_status['filled'])
        exitPrice = (existing_position.exitAmount * existing_position.exitPrice + float(order_status['average']) * float(order_status['filled'])) / exitAmount
    
    PnL = singleTradeROI = PnLRatio = None

    if exitAmount == existing_position.amount:
        PnL = (exitPrice * exitAmount - existing_position.entryPrice * existing_position.amount - (exitPrice * exitAmount + existing_position.entryPrice * existing_position.amount) * 0.00018)
        # to do: fees 是粗略估计，还差资金费用没计算在内
        singleTradeROI = PnL / existing_position.stakeAmount if existing_position.stakeAmount else None
        PnLRatio = PnL / totalCapital
        totalCapital += PnL if PnL else 0

    exitDate = datetime.strptime(order_status['datetime'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")

    try:    
        if existing_position:
            existing_position.updateTime = exitDate
            existing_position.updateTimeStamp = order_status['timestamp']
            existing_position.exitDate = exitDate
            existing_position.exitPrice = exitPrice
            existing_position.exitAmount = exitAmount
            existing_position.PnL = PnL
            existing_position.PnLRatio = PnLRatio
            existing_position.singleTradeROI = singleTradeROI
            existing_position.totalCapital = totalCapital
            session.commit()
    
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Error during save order {order_status} to db: {e}")
    finally:
        session.close()

async def watch_order_book(symbol, queue_buy_price, queue_sell_price, exchange_usdm):
    last_buy_price = None
    last_sell_price = None

    while True:
        try:
            orderbook = await exchange_usdm.watch_order_book(symbol)
            buy_price = orderbook['bids'][0][0]  
            sell_price = orderbook['asks'][0][0] 
            
            if buy_price != last_buy_price:
                await queue_buy_price.put(buy_price)
                last_buy_price = buy_price  # 更新最后的买入价格

            if sell_price != last_sell_price:
                await queue_sell_price.put(sell_price)
                last_sell_price = sell_price  # 更新最后的卖出价格
                
        except ccxt.BaseError as e:
            logging.error(f"Error opening position for {symbol}: {e}")

async def entry_position(symbol_info, queue_buy_price, exchange_usdm):
    symbol = symbol_info['symbol']
    stakeAmount = float(symbol_info['stakeAmount'])
    leverage = int(symbol_info['leverage'])
    decimalPlaces = int(symbol_info['decimalPlaces'])
    capitalPercent = float(symbol_info['capitalPercent'])
                
    try:           
        buy_price = await queue_buy_price.get()    
        amount = stakeAmount * leverage / buy_price
        adjusted_amount = round(amount, decimalPlaces)
        if decimalPlaces == 0:
            adjusted_amount = int(adjusted_amount)

        balance = await exchange_usdm.fetch_balance()
        available_balance = float(balance['info']['availableBalance'])
        if available_balance < stakeAmount:
            logging.error(f"U 本位合约余额不足，无法下单: {symbol}")
            return

        setleverage = await exchange_usdm.set_leverage(leverage, symbol) 
        logging.info(f"Set leverage: {setleverage}")
        setmargin = await exchange_usdm.setMarginMode('cross', symbol)
        logging.info(f"Set margin mode: {setmargin}")
        order = await exchange_usdm.createOrder(symbol, 'limit', 'buy', adjusted_amount, buy_price)  
        order_to_database(order, symbol, leverage, stakeAmount, capitalPercent)

        await asyncio.wait_for(check_order_filled(exchange_usdm, order, symbol, buy_price, leverage, stakeAmount, capitalPercent), timeout=600)               

    except ccxt.BaseError as e:
        logging.error(f"Error opening position for {symbol}: {e}")
        return
    
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
            order_to_database(order_status, symbol, leverage, stakeAmount, capitalPercent)
            break  # 订单完全成交，退出循环 

async def exit_position(symbol_info, queue_sell_price, exchange_usdm):
    symbol = symbol_info['symbol']
    amount = symbol_info['amount']

    try:
        sell_price = await queue_sell_price.get()  
        # setmode = await exchange_usdm.setPositionMode(False, symbol)
        order = await exchange_usdm.createOrder(symbol, 'limit', 'sell', amount, sell_price, {'reduceOnly': 'true'}) 

        await asyncio.wait_for(check_close_order_filled(exchange_usdm, order, symbol, sell_price), timeout=600) 

    except ccxt.BaseError as e:
        logging.error(f"Error closeing position for {symbol}: {e}")
        return
    
    except asyncio.TimeoutError:
        canceled = await exchange_usdm.cancelOrder(order['id'], symbol)
        logging.info(f"Unfilled order canceled: {canceled}")
        order_status = await exchange_usdm.fetchOrder(order['id'], symbol)
        remaining_amount = order_status['remaining']
        market_order = await exchange_usdm.createOrder(symbol, 'market', 'sell', remaining_amount)  
        logging.info(f"Market order placed for the remaining amount: {market_order}")

async def check_close_order_filled(exchange_usdm, order, symbol, sell_price):
    while True:         
        order_status = await exchange_usdm.fetchOrder(order['id'], symbol)
        remaining_amount = order_status['remaining']
        logging.info(f"Order execution status: {order_status}")

        if remaining_amount > 0:                                        
            logging.info(sell_price)
            logging.info(f"Modifying order {order['id']}: Remaining amount {remaining_amount}, New sell price {sell_price}")
            edited_order = await exchange_usdm.editOrder(order['id'], symbol, 'limit', 'sell', remaining_amount, sell_price)
            logging.info(f"Order modified: {edited_order}")
            await asyncio.sleep(2)  # 等待2秒再次检查
        else:
            logging.info("Order fully filled.")
            break  # 订单完全成交，退出循环            

async def update_trade():
    perpetual_positions = retrieve_positions(tradeType='PERPETUAL')
    delivery_positions = retrieve_positions(tradeType='DELIVERY')
    api_data = perpetual_positions + delivery_positions

    if not api_data:
        return
    db_symbols_timestamps = get_all_symbols_and_latest_timestamps()
    new_symbols_timestamps = {item['symbol']: item['updateTimeStamp'] for item in api_data}
    
    # 比较数据库和API/tweet的symbol，确定新增和减少的symbol
    new_symbols = set(new_symbols_timestamps.keys()) - set(db_symbols_timestamps.keys())
    closed_symbols = set(db_symbols_timestamps.keys()) - set(new_symbols_timestamps.keys())
    
    # 确定需要更新的symbols
    updated_symbols = {symbol for symbol, timestamp in new_symbols_timestamps.items()
                       if symbol in db_symbols_timestamps and timestamp > db_symbols_timestamps[symbol]}
    
    # 根据情况调用相应函数
    if new_symbols:
        symbols_info = [{'symbol': symbol, 'before_capital_percent': 0} for symbol in new_symbols]
        add_new_trades(new_symbols, api_data, source='leaderboard')
        position_df = calculate_positions()
        trade_symbols_info = get_symbol_info(symbols_info, position_df)
        await trade(trade_symbols_info, 'entry')

    if closed_symbols:
        close_existing_trades(closed_symbols)
        symbols_info = fetch_amounts_for_symbols()
        await trade(trade_symbols_info, 'exit')

    if updated_symbols:
        session = Session()
        for symbol in updated_symbols:  # 遍历需要更新的symbols
            item = next((item for item in api_data if item['symbol'] == symbol), None)
            current_position = session.query(Position).filter(Position.symbol == symbol).first()
            if item['amount'] > current_position.amount:
                symbol_info = get_symbols_percent(symbol)
                add_position(symbol, item)
                position_df = calculate_positions()
                trade_symbols_info = get_symbol_info(symbol_info, position_df)
                await trade(trade_symbols_info, 'entry')
            elif item['amount'] < current_position.amount:
                symbol_info = get_symbols_percent(symbol)
                reduce_position(symbol, item)
            elif item['leverage'] != current_position.leverage:
                change_leverage(symbol, item)

    update_sync(api_data)

async def trade(trade_symbols_info, action):
    exchange_usdm = ccxtpro.binanceusdm({
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,  
    })
    tasks = []
    queue_buy_price = asyncio.Queue()  
    queue_sell_price = asyncio.Queue()    
    for trade_symbol_info in trade_symbols_info:
        symbol = trade_symbol_info['symbol']
        watcher = asyncio.create_task(watch_order_book(symbol, queue_buy_price, queue_sell_price, exchange_usdm))   
        tasks.append(watcher)

        if action == 'entry':
            trader = asyncio.create_task(entry_position(trade_symbol_info, queue_buy_price, exchange_usdm))
        elif action == 'exit':
            trader = asyncio.create_task(exit_position(trade_symbol_info, queue_sell_price, exchange_usdm))    
        tasks.append(trader)
    
    await asyncio.gather(*tasks)
    await exchange_usdm.close()

if __name__ == '__main__':
    init_db()
    initial_insert_positions()
    retrieve_performance()
    calculate_positions() #可以每当仓位更新时调一次

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=update_trade, trigger='interval', minutes=1, next_run_time=datetime.now())
    scheduler.start()

    # 保持主线程运行，防止程序结束
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()