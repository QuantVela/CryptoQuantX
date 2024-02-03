import requests
import json
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
import time

# 可参考项目
# https://github.com/tpmmthomas/binance-copy-trade-bot/blob/14133d86434271aae20a1a06b9e926350c33213e/Binance/bparser.py
# https://github.com/DogeIII/Binance-Leaderboard-CopyTrading/blob/main/bot.py

Base = declarative_base()
engine = create_engine('sqlite:///SmartestMoney.db')
Session = sessionmaker(bind=engine)

class Position(Base):
    __tablename__ = 'positions'

    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    entryPrice = Column(Float)
    markPrice = Column(Float)
    pnl = Column(Float)
    roe = Column(Float)
    updateTime = Column(String)
    amount = Column(Float)
    updateTimeStamp = Column(Integer, unique=True)
    yellow = Column(Boolean)
    tradeBefore = Column(Boolean)
    leverage = Column(Integer)

def init_db():
    Base.metadata.create_all(engine)

def retrieve_positions(tradeType):
    try:
        url = 'https://binance-futures-leaderboard1.p.rapidapi.com/v1/getOtherPosition'
        querystring = {'encryptedUid':'1FB04E31362DEED9CAA1C7EF8A771B8A', 'tradeType':tradeType} #PERPETUAL 是 U 本位，DELIVERY是币本位
        headers = {
            'X-RapidAPI-Key': '9cb3faa370msh6ea8f37e65e4111p162897jsn1e9c3cc8f936',
            'X-RapidAPI-Host': 'binance-futures-leaderboard1.p.rapidapi.com'
        }
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 200:
            data = response.json().get('data', {})
            if data.get('otherPositionRetList') is None:
                return []
            else:
                return data['otherPositionRetList']
        else:
            return []
    except Exception as e:
        print('Error retrieving positions', e)   
        return []

def get_latest_updateTimeStamp(symbol):
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
            updateTime=str(item['updateTime']),  # 假设updateTime需要转换为字符串
            amount=item['amount'], 
            updateTimeStamp=item['updateTimeStamp'],
            yellow=item['yellow'], 
            tradeBefore=item['tradeBefore'], 
            leverage=item['leverage']
        )
        session.add(position)
    
    try:
        # 提交到数据库
        session.commit()
    except Exception as e:
        print(f"Error while inserting positions: {e}")
        session.rollback()
    finally:
        session.close()

def add_new_trades(new_symbols, api_data):
    session = Session()
    try:
        for item in api_data:
            if item['symbol'] in new_symbols:
                new_position = Position(
                    symbol=item['symbol'],
                    entryPrice=item['entryPrice'],
                    markPrice=item['markPrice'],
                    pnl=item['pnl'],
                    roe=item['roe'],
                    updateTime=str(item['updateTime']),
                    amount=item['amount'],
                    updateTimeStamp=item['updateTimeStamp'],
                    yellow=item['yellow'],
                    tradeBefore=item['tradeBefore'],
                    leverage=item['leverage']
                )
                session.add(new_position)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error adding new trades: {e}")
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
        print(f"Error closing existing trades: {e}")
    finally:
        session.close()

def update_existing_trades(api_data, updated_symbols):
    session = Session()
    try:
        for symbol in updated_symbols:  # 遍历需要更新的symbols
            item = next((item for item in api_data if item['symbol'] == symbol), None)
            if not item:
                continue  # 如果在api_data中找不到这个symbol的记录，跳过

            current_position = session.query(Position).filter(Position.symbol == symbol).first()
            if not current_position:
                continue  # 如果数据库中没有这个symbol的记录，跳过

            # 检查amount的变化
            if item['amount'] > current_position.amount:
                print(f"⬆️ Increased a position for {symbol}")
                current_position.amount = item['amount']
            elif item['amount'] < current_position.amount:
                print(f"⬇️ Decreased a position for {symbol}")
                current_position.amount = item['amount']

            # 检查leverage的变化
            if 'leverage' in item and item['leverage'] > current_position.leverage:
                print(f"🔺 Increased leverage for {symbol}")
                current_position.leverage = item['leverage']
            elif 'leverage' in item and item['leverage'] < current_position.leverage:
                print(f"🔻 Decreased leverage for {symbol}")
                current_position.leverage = item['leverage']

            # 更新其他字段
            current_position.entryPrice = item['entryPrice']
            current_position.markPrice = item['markPrice']
            current_position.pnl = item['pnl']
            current_position.roe = item['roe']
            current_position.updateTime = str(item['updateTime'])
            current_position.updateTimeStamp = item['updateTimeStamp']
            current_position.yellow = item['yellow']
            current_position.tradeBefore = item['tradeBefore']

        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error updating existing trades: {e}")
    finally:
        session.close()

def update_trade():
    db_symbols_timestamps = get_all_symbols_and_latest_timestamps()

    perpetual_positions = retrieve_positions(tradeType='PERPETUAL')
    delivery_positions = retrieve_positions(tradeType='DELIVERY')
    api_data = perpetual_positions + delivery_positions

    api_symbols_timestamps = {item['symbol']: item['updateTimeStamp'] for item in api_data}
    
    # 比较数据库和API的symbol，确定新增和减少的symbol
    new_symbols = set(api_symbols_timestamps.keys()) - set(db_symbols_timestamps.keys())
    closed_symbols = set(db_symbols_timestamps.keys()) - set(api_symbols_timestamps.keys())
    
    # 确定需要更新的symbols
    updated_symbols = {symbol for symbol, timestamp in api_symbols_timestamps.items()
                       if symbol in db_symbols_timestamps and timestamp > db_symbols_timestamps[symbol]}
    
    # 根据情况调用相应函数
    if new_symbols:
        add_new_trades(new_symbols, api_data)
    if closed_symbols:
        close_existing_trades(closed_symbols)
    if updated_symbols:
        update_existing_trades(updated_symbols, api_data)

init_db()
initial_insert_positions()
while True:
    update_trade()
    time.sleep(7200)



