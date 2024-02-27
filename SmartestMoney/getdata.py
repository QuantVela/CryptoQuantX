import requests
import json
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import time
from dotenv import load_dotenv
import os

# API: https://rapidapi.com/DevNullZero/api/binance-futures-leaderboard1
# 可参考项目
# https://github.com/tpmmthomas/binance-copy-trade-bot/blob/14133d86434271aae20a1a06b9e926350c33213e/Binance/bparser.py
# https://github.com/DogeIII/Binance-Leaderboard-CopyTrading/blob/main/bot.py

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

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
    source = Column(String)

class TweetUpdateTime(Base):
    __tablename__ = 'tweet_update_time'

    id = Column(Integer, primary_key=True)
    updateTimeStamp = Column(Integer, unique=True)
    updateTime = Column(String)

def init_db():
    Base.metadata.create_all(engine)

def retrieve_positions(tradeType):
    '''获取排行榜里的仓位信息'''
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

def store_update_time(session, timestamp, formatted_time):
    try:
        update_time_obj = session.query(TweetUpdateTime).first()
        if update_time_obj:
            update_time_obj.updateTimeStamp = timestamp
            update_time_obj.updateTime = formatted_time
        else:
            new_update_time = TweetUpdateTime(updateTimeStamp=timestamp, updateTime=formatted_time)
            session.add(new_update_time)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error updating/inserting tweet update time: {e}")
    finally:
        session.close()

def handle_new_tweets(data, session, last_update_timestamp):
    new_tweets = [tweet for tweet in data["timeline"] if datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y').timestamp() > last_update_timestamp]
    trade_updates_phrases = ['Trade Updates', 'Trade Update', 'trade update', 'trade updates', 'TRADE UPDATE', 'TRADE UPDATES']
    position_update_phrases = ['Position Update', 'Position Updates', 'Portfolio Update', 'Portfolio Updates',
                               'position update', 'position updates', 'portfolio update', 'portfolio updates',
                               'POSITION UPDATE', 'POSITION UPDATES', 'PORTFOLIO UPDATE', 'PORTFOLIO UPDATES',
                               'Positions Update', 'positions update', 'POSITIONS UPDATE']
    
    trade = [{
        'tweet_id': tweet['tweet_id'],
        'created_at': tweet['created_at'],
        'text': tweet['text'],
        'media': tweet['media']
    } for tweet in data['timeline']
              if any(phrase in tweet['text'] for phrase in trade_updates_phrases)
              and not tweet['text'].startswith('RT @')
              and 'media' in tweet]

    positions = [{
        'tweet_id': tweet['tweet_id'],
        'created_at': tweet['created_at'],
        'text': tweet['text'],
        'media': tweet['media']
    } for tweet in data['timeline']
                  if any(phrase in tweet['text'] for phrase in position_update_phrases)
                  and not tweet['text'].startswith('RT @')
                  and 'media' in tweet]
    #不为空时，非gif的图像逐个识别
    print(trade, positions)

def retrieve_tweets():
    session = Session()
    try:
        url = "https://twitter-api45.p.rapidapi.com/timeline.php"
        querystring = {"screenname":"smartestmoney_","rest_id":"1641995815983140865"}
        headers = {
            "X-RapidAPI-Key": "9cb3faa370msh6ea8f37e65e4111p162897jsn1e9c3cc8f936",
            "X-RapidAPI-Host": "twitter-api45.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 200:
            data = response.json()
            if data:
                created_at = data["timeline"][0]["created_at"]
                tweet_time = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')
                timestamp = tweet_time.timestamp()
                formatted_time = tweet_time.strftime('%Y-%m-%d %H:%M:%S')
                update_time_obj = session.query(TweetUpdateTime).first()

                if update_time_obj and update_time_obj.updateTimeStamp:
                    if timestamp > update_time_obj.updateTimeStamp: #发现更新推文
                        store_update_time(session, timestamp, formatted_time)
                        handle_new_tweets(data, session, update_time_obj.updateTimeStamp)

                else: # 首次运行或没有记录                   
                    store_update_time(session, timestamp, formatted_time)

                session.close()
            else:
                print("No data found in response.")
                return []
    except Exception as e:
        print('Error retrieving tweets', e)
        return []

def call_openai(prompt, url):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    payload = {
        "model": "gpt-4-vision-preview",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": url,
                    },
                ],
            }
        ],
        "temperature": 0
    }

    response = requests.post(
        "https://openai-proxy-self-ten.vercel.app/v1/chat/completions",
        headers=headers,
        data=json.dumps(payload)
    )
    response_json = response.json()
    response_content = response_json['choices'][0]['message']['content']

    return response_content

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
            updateTime=datetime(*item['updateTime'][:6]).strftime('%Y-%m-%d %H:%M:%S'),
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
                    updateTime=datetime(*item['updateTime'][:6]).strftime('%Y-%m-%d %H:%M:%S'),
                    amount=item['amount'],
                    updateTimeStamp=item['updateTimeStamp'],
                    yellow=item['yellow'],
                    tradeBefore=item['tradeBefore'],
                    leverage=item['leverage'],
                    source='leaderboard'
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
# initial_insert_positions()
retrieve_tweets()
# while True:
#     update_trade()
#     time.sleep(7200)



