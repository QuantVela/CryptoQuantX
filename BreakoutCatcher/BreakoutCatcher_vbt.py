import numpy as np
import pandas as pd
from datetime import datetime
import vectorbtpro as vbt
import matplotlib.pyplot as plt
import os
from numba import njit
import time
import json
from pandas import Timestamp
from pandas import Timedelta
from json import JSONEncoder
import quantstats as qs
import warnings
import talib

# 策略规则：
# 币对筛选：11 日累计成交量前 32 个，去除稳定币和 BTC, 按 width 排序（按volume排序是 5673，着急可以先上）
# width 定义：SMA20 的 2 个标准差的布林带宽度，取前一日的值，0-1 之间
# 进场：突破 30 日内最高价，且 BTC > MA50
# 出场：BTC < MA50 或第三天收盘时
# 资金均分 10 份，最多持仓 10 个币
# 回报：5918%,回撤：28%,胜率：50% 

start_time = time.time()
warnings.filterwarnings('ignore', category=FutureWarning)
change_date = pd.to_datetime('2020-09-04 00:00:00+00:00')

class CustomEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Timestamp):
            # 将Timestamp转换为字符串
            return obj.strftime('%Y-%m-%d %H:%M:%S %Z')
        return JSONEncoder.default(self, obj)

def select_top_n(row):
    if row['date'] <= change_date:
        # 如果币种数量小于等于30，选择前20%
        return row['rank'] <= row['coin_count'] * 0.2
    else:
        # 如果币种数量大于30，选择前30个
        return row['rank'] <= 32
    
def pair_filter(data_folder, start_date, end_date):
    all_data = []

    # 合并所有Feather文件数据到一个DataFrame
    for filename in os.listdir(data_folder):
        if filename.endswith('.feather'):
            coin_pair = filename.split('-')[0] 
            file_path = os.path.join(data_folder, filename)
            try:
                data = pd.read_feather(file_path)
                if 'date' in data.columns:
                    data['coin_pair'] = coin_pair  # 添加币对列
                    all_data.append(data)
            except Exception as e:
                print(f"Error reading {filename}: {e}")

    # 检查并合并DataFrame
    df = pd.concat([df for df in all_data if not df.empty])

    df['date'] = pd.to_datetime(df['date'], utc=True)
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
    df['turnover'] = (df['open'] + df['high'] + df['low']) / 3 * df['volume']

    # 对DataFrame进行分组并滚动计算x日累计成交额
    df = df.sort_values(by=['coin_pair', 'date'])
    df['3_day_turnover'] = df.groupby('coin_pair')['turnover'].rolling(11, min_periods=1).sum().shift(1).reset_index(level=0, drop=True)

    # 排序并筛选每个日期的前20%
    df['rank'] = df.groupby('date')['3_day_turnover'].rank("dense", ascending=False)
    # df_top20 = df[df['rank'] <= df.groupby('date')['rank'].transform(lambda x: x.size // 5)]
    df['coin_count'] = df.groupby('date')['coin_pair'].transform('count') # 在排名之前，为每个日期计算币种数量
    df['select'] = df.apply(select_top_n, axis=1) # 应用 select_top_n 函数来筛选DataFrame
    df_top = df[df['select']]

    # 排序DataFrame
    df_top_sorted = df_top.sort_values(by=['date', 'rank'], ascending=[True, True])

    # 排除特定币对
    blacklist = ['USDC_USDT', 'BUSD_USDT', 'TUSD_USDT', 'FDUSD_USDT']
    df_filtered = df_top_sorted[~df_top_sorted['coin_pair'].isin(blacklist)]

    return df_filtered[['date', 'coin_pair', 'rank']]    

def build_ohlcv_dict(df_filtered):
    ohlcv_dict = {}
    unique_coin_pairs = df_filtered['coin_pair'].unique()

    for coin_pair in unique_coin_pairs:
        feather_path = f"../ft_userdata/user_data/data/binance/allpairs/1h/{coin_pair}-1h.feather"
        if os.path.exists(feather_path):
            df_ohlcv = pd.read_feather(feather_path)
            df_ohlcv.set_index('date', inplace=True)
            ohlcv_dict[coin_pair] = df_ohlcv
        else:
            print(f"File not found: {feather_path}")

    return ohlcv_dict

def get_highest(high, period=30*24):
    highest = high.rolling(period).max()
    breakout = high >= highest
    return highest, breakout

def get_lowest(low, period=21*24):
    lowest = low.rolling(period).min()
    breakdown = low <= lowest
    return lowest, breakdown

# 设置数据文件夹路径和日期范围
data_folder = '../ft_userdata/user_data/data/binance/allpairs/1d/'
start_date = pd.to_datetime("2017-08-18 00:00:00+00:00")
end_date = pd.to_datetime("2024-01-19 00:00:00+00:00")

# 计算3日成交额
df_filtered = pair_filter(data_folder, start_date, end_date)
ohlcv_dict = build_ohlcv_dict(df_filtered)
data = vbt.Data.from_data(ohlcv_dict, silence_warnings=True)

open = data.get('Open')
high = data.get('High')
close = data.get('Close')
low = data.get('Low')
close_1d = close.resample('D').last()

def cal_width(close_1d):
    bbands = vbt.BBANDS.run(close_1d, window=20)
    width = bbands.bandwidth
    width.columns = width.columns.droplevel('bb_window')
    width = width.shift(1)
    return width

# 使用 groupby 按 date 分组，并计算每组的 coin_pair 数量
# coin_pair_stats_by_date = df_filtered.groupby('date').agg(
#     count=('coin_pair', 'size'),  # 计算每天的币对数量
#     coin_pairs=('coin_pair', lambda x: ', '.join(x))  # 将每天的币对合并成一个字符串列表
# ).reset_index()

# # 将统计结果保存到 CSV 文件中
# coin_pair_stats_by_date.to_csv('coin_pair.csv', index=False)
# print("okk")

def entry_signal():
    #breakout
    high_30, breakout = get_highest(high)
    #btc filter
    btc_close_1d = close['BTC_USDT'].resample('D').last()
    btc_ma50_1d = vbt.MA.run(btc_close_1d, 50)
    btc_filter_1d = btc_ma50_1d.ma_below(btc_close_1d)
    btc_filter_at_23 = btc_filter_1d.shift(1, freq='23H')
    btc_filter_1h = btc_filter_at_23.reindex(breakout.index, method='ffill')
    btc_filter_1h.fillna(False, inplace=True)
    #coin filter
    coin_filter = pd.DataFrame(index=breakout.index, columns=breakout.columns)
    for _, row in df_filtered.iterrows():
        date = row['date']
        coin_pair = row['coin_pair']
        if coin_pair in coin_filter.columns:
            coin_filter.at[date, coin_pair] = True
    coin_filter = coin_filter.groupby(coin_filter.index.date).ffill() #把1d信号填充到1h
    coin_filter.fillna(False, inplace=True)
    coin_filter['BTC_USDT'] = False
    coin_filter = coin_filter.iloc[:-1]

    mask = breakout.vbt & coin_filter 
    mask_final = mask.vbt & btc_filter_1h
    return mask_final

mask = entry_signal()
# slope_df = cal_squeeze(close_1d)
width = cal_width(close_1d)

def exit_signal(mask):
    btc_close_1d = close['BTC_USDT'].resample('D').last()
    btc_ma50_1d = vbt.MA.run(btc_close_1d, 50)
    btc_bear_filter_1d = btc_ma50_1d.ma_above(btc_close_1d)
    btc_bear_filter_at_23 = btc_bear_filter_1d.shift(1, freq='23H')
    btc_bear_filter_1h = btc_bear_filter_at_23.reindex(mask.index, method='ffill')
    btc_bear_filter_1h.fillna(False, inplace=True)
    return btc_bear_filter_1h

btc_bear_filter_1h = exit_signal(mask)

entries = pd.DataFrame(False, index=mask.index, columns=mask.columns)
exits = pd.DataFrame(False, index=mask.index, columns=mask.columns)
exits_filter = exits.vbt | btc_bear_filter_1h
holdings = {}
lowest_price, is_breakdown = get_lowest(low)
exited_coins = set()
size = pd.DataFrame(0, index=mask.index, columns=mask.columns)
capital_columns = ['Remaining Cash', 'Available Cash', 'Asset Value']
capital_df = pd.DataFrame(np.nan, index=mask.index, columns=capital_columns)
initial_cash = 10000
fees = 0.001
capital_df.iloc[0] = [initial_cash, initial_cash * 0.99, initial_cash]  # 初始资金分配

def update_capital_and_exit(date, coin_pair, exit_price, exit_size):
    exits.at[date, coin_pair] = True
    size.at[date, coin_pair] = -exit_size
    # print(coin_pair, "exit size",exit_size)
    capital_gain = exit_price * exit_size * (1 - fees)
    capital_df.at[date, 'Remaining Cash'] += capital_gain
    print(coin_pair,"Remaining Cash",capital_df.at[date, 'Remaining Cash'])
    capital_df.at[date, 'Available Cash'] = capital_df.at[date, 'Remaining Cash'] * 0.99
    del holdings[coin_pair]
    asset_value_sum = sum(
        holdings[coin]['entry_price'] * holdings[coin]['size'] for coin in holdings
    ) 
    capital_df.at[date, 'Asset Value'] = capital_df.at[date, 'Remaining Cash'] + asset_value_sum   
    exited_coins.add(coin_pair)
    # print("Coins in holdings:", list(holdings.keys()))

def update_capital_and_entry(date, coin_pair, current_price, stake_amount):
    available_cash = capital_df.at[date, 'Available Cash']
    
    if stake_amount > available_cash:
        print(f"{coin_pair}: Skipped due to insufficient available cash.")
        return

    entries.at[date, coin_pair] = True
    trade_size = stake_amount / current_price
    size.at[date, coin_pair] = trade_size

    # 更新 holdings
    holdings[coin_pair] = {'entry_date': date, 'entry_price': current_price, 'size': trade_size}

    # 更新 capital_df
    capital_df.at[date, 'Remaining Cash'] -= trade_size * current_price * (1 + fees)
    capital_df.at[date, 'Available Cash'] = capital_df.at[date, 'Remaining Cash'] * 0.99

    # 计算新的 Asset Value
    asset_value = capital_df.at[date, 'Remaining Cash'] + sum(
        holdings[coin]['entry_price'] * holdings[coin]['size'] for coin in holdings
    )
    capital_df.at[date, 'Asset Value'] = asset_value

for date, signals_on_date in mask.iterrows():  # 遍历 mask 中的每个日期（按小时）

    exited_coins.clear()

    if pd.isna(capital_df.loc[date, 'Remaining Cash']):
        # 找到最近的非 NaN 行
        last_valid_index = capital_df.loc[:date].last_valid_index()
        capital_df.loc[date] = capital_df.loc[last_valid_index]
        # print("😂update nan",capital_df.loc[date])

    if date == mask.index[-1]:
        for coin_pair in list(holdings):
            # 以最后一条数据的收盘价强制平仓退出
            exit_price = close.at[date, coin_pair]
            exit_size = holdings[coin_pair]['size']
            update_capital_and_exit(date, coin_pair, exit_price, exit_size)
            print(coin_pair, "update exit size cas force",capital_df.loc[date])
        continue

    for coin_pair in list(holdings):  # 使用list来避免在循环中修改字典
        entry_date = holdings[coin_pair]['entry_date']
        entry_price = holdings[coin_pair]['entry_price']
        exit_size = holdings[coin_pair]['size']
        current_price = close.at[date, coin_pair]
        
        # if current_price <= 0.5 * entry_price or is_breakdown.at[date, coin_pair]:
        if is_breakdown.at[date, coin_pair]:
            update_capital_and_exit(date, coin_pair, current_price, exit_size)
            print(coin_pair, "update exit size cas low21",capital_df.loc[date])

        # 检查exits_filter在此日期和币对下的值是否为True
        elif exits_filter.at[date, coin_pair]:
            update_capital_and_exit(date, coin_pair, current_price, exit_size)
            print(f"{coin_pair} exit because BTC is below MA50", capital_df.loc[date])

        elif date - entry_date > Timedelta(days=2) and date.hour == 0:
            update_capital_and_exit(date, coin_pair, current_price, exit_size)
            print(coin_pair, "update exit size cas 3days",capital_df.loc[date])

    signals = signals_on_date[signals_on_date].index.tolist()  # 检查当前小时有哪些币对发出了入场信号
    if not signals or len(holdings) >= 10:
        continue
    date_daily = pd.to_datetime(date).normalize() # 将 mask 的按小时日期转换为按天日期，以便与 df_filtered 对齐
    # widths_on_date = width.loc[date_daily, signals]
    # sorted_widths = sorted(widths_on_date.items(), key=lambda x: x[1])
    # sorted_signals = [signal for signal, _ in sorted_widths]    
    rank_on_date = df_filtered[df_filtered['date'] == date_daily]
    rank_on_date = rank_on_date[rank_on_date['coin_pair'].isin(signals)]
    sorted_signals = rank_on_date.sort_values('rank')['coin_pair'].tolist()

    for coin_pair in sorted_signals:
        if len(holdings) < 10 and coin_pair not in holdings and coin_pair not in exited_coins:
            # 使用原始的按小时的日期时间索引进行记录
            asset_value = capital_df.at[date, 'Asset Value']
            stake_amount = asset_value / 10
            entry_price = close.at[date, coin_pair]
            # print(json.dumps(holdings, indent=4, cls=CustomEncoder))
            update_capital_and_entry(date, coin_pair, entry_price, stake_amount)
            print(coin_pair, "update entry size",capital_df.loc[date])

exits = exits.fillna(False)

# pf = vbt.Portfolio.from_signals(
#     close=close, 
#     entries=entries, 
#     exits=exits, 
#     init_cash=10000,
#     size=1/10,
#     size_type='valuepercent',
#     cash_sharing=True,
#     direction='longonly',
#     fees=0.001,
#     freq='1h'
# )
pf = vbt.Portfolio.from_orders(
    close=close, 
    price=close,
    size=size,
    size_type='amount',
    cash_sharing=True,
    init_cash=10000,
    direction='longonly',
    fees=0.001,
    freq='1h'
)

print(pf.stats())    

daily_returns = pf.daily_returns
btc_returns = close['BTC_USDT'].pct_change(fill_method=None)
btc_returns.fillna(0, inplace=True)
btc_returns.name = 'btc'
daily_returns.index = daily_returns.index.tz_localize(None)
btc_returns.index = btc_returns.index.tz_localize(None)

qs.reports.html(daily_returns, benchmark=btc_returns, output='report_fo.html')

orders = pf.orders.records_readable
csv_path = 'orders_date_fo.csv'
tradelog_csv_path = 'tradelog_date_fo.csv' 
orders.to_csv(csv_path)
print("csv done")

def gen_tradelog(csv_path, tradelog_csv_path):
    csv_path = csv_path
    orders_df = pd.read_csv(csv_path)
    trades = []

    for i in range(0, len(orders_df), 2):
        buy_row = orders_df.iloc[i]
        sell_row = orders_df.iloc[i + 1]

        trade = {
            '交易币对': buy_row['Column'],
            '买入时间': buy_row['Fill Index'] if 'Fill Index' in buy_row else buy_row['Index'],
            '买入价格': buy_row['Price'],
            '买入数量': buy_row['Size'],
            '卖出时间': sell_row['Fill Index'] if 'Fill Index' in sell_row else sell_row['Index'],
            '卖出价格': sell_row['Price'],
            '卖出数量': sell_row['Size'],
            'USDT Value': buy_row['Price'] * buy_row['Size'],
            'Fees': buy_row['Fees'] + sell_row['Fees']
        }
        trades.append(trade)

    tradelog_df = pd.DataFrame(trades)
    tradelog_df.sort_values(by='买入时间', inplace=True)
    initial_capital = 10000
    total_capital = initial_capital
    pnls = []
    total_capitals = []

    for index, row in tradelog_df.iterrows():
        pnl = (row['卖出价格'] * row['卖出数量']) - (row['买入价格'] * row['买入数量']) - row['Fees']
        pnls.append(pnl)
        total_capital += pnl
        total_capitals.append(total_capital)

    tradelog_df['PnL'] = pnls
    tradelog_df['总资金'] = total_capitals
    tradelog_df['PnL Ratio'] = (tradelog_df['PnL'] / tradelog_df['总资金']) * 100
    tradelog_df['单笔收益率'] = (tradelog_df['PnL'] / tradelog_df['USDT Value']) * 100

    tradelog_df['PnL'] = tradelog_df['PnL'].apply(lambda x: f"{x:.2f}")
    tradelog_df['总资金'] = tradelog_df['总资金'].apply(lambda x: f"{x:.2f}")
    tradelog_df['PnL Ratio'] = tradelog_df['PnL Ratio'].apply(lambda x: f"{x:.2f}%")
    tradelog_df['单笔收益率'] = tradelog_df['单笔收益率'].apply(lambda x: f"{x:.2f}%")
 
    tradelog_df.to_csv(tradelog_csv_path, index=False)

    print("Tradelog CSV has been generated successfully.")

gen_tradelog(csv_path, tradelog_csv_path)
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")










