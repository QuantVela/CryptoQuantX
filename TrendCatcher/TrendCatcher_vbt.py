import numpy as np
import pandas as pd
import vectorbtpro as vbt
import os
import time
import json
from pandas import Timestamp
from pandas import Timedelta
from json import JSONEncoder
import quantstats as qs
import warnings

# 策略规则：
# 币对筛选：3 日累计成交量前 32 个，去除稳定币
# 进场：收盘价上穿 ma20，且 BTC > MA50
# 出场：BTC < MA50 或收盘价下穿 ma20
# 资金：按 ATR 把资金分为 10 份，最多持仓 10 个币。每上涨 0.25ATR 加仓，可加仓 2 次。
# 回报：6858%,回撤：40%,胜率：23%，还有一个每 0.5ATR 加仓 1 次的数据差不多，但拿着没那么舒服

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
        # 币种数量小于 30 的日期
        return row['rank'] <= row['coin_count'] * 0.2
    else:
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

    # 对DataFrame进行分组并滚动计算3日累计成交额
    df = df.sort_values(by=['coin_pair', 'date'])
    df['3_day_turnover'] = df.groupby('coin_pair')['turnover'].rolling(3, min_periods=1).sum().shift(1).reset_index(level=0, drop=True)

    # 排序并筛选每个日期的前20%
    df['rank'] = df.groupby('date')['3_day_turnover'].rank("dense", ascending=False)
    # df_top20 = df[df['rank'] <= df.groupby('date')['rank'].transform(lambda x: x.size // 5)]
    df['coin_count'] = df.groupby('date')['coin_pair'].transform('count') # 在排名之前，为每个日期计算币种数量
    df['select'] = df.apply(select_top_n, axis=1) # 应用 select_top_n 函数来筛选DataFrame
    df_top = df[df['select']]

    # 排序DataFrame
    df_top_sorted = df_top.sort_values(by=['date', 'rank'], ascending=[True, True])

    # 排除特定币对
    blacklist = ['USDC_USDT', 'BUSD_USDT', 'TUSD_USDT', 'FDUSD_USDT', 'ERD_USDT']
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

# 设置数据文件夹路径和日期范围
data_folder = '../ft_userdata/user_data/data/binance/allpairs/1d/'
start_date = pd.to_datetime("2017-08-18 00:00:00+00:00")
end_date = pd.to_datetime("2024-01-19 00:00:00+00:00")

# 计算3日成交额
df_filtered = pair_filter(data_folder, start_date, end_date)
ohlcv_dict = build_ohlcv_dict(df_filtered)
data = vbt.Data.from_data(ohlcv_dict, silence_warnings=True)

# 使用 groupby 按 date 分组，并计算每组的 coin_pair 数量
coin_pair_stats_by_date = df_filtered.groupby('date').agg(
    count=('coin_pair', 'size'),  # 计算每天的币对数量
    coin_pairs=('coin_pair', lambda x: ', '.join(x))  # 将每天的币对合并成一个字符串列表
).reset_index()

# 将统计结果保存到 CSV 文件中
coin_pair_stats_by_date.to_csv('coin_pair_stats_by_date.csv', index=False)
df_filtered.to_csv('df_filtered.csv', index=False)
print("okk")

open = data.get('Open')
high = data.get('High')
close = data.get('Close')
low = data.get('Low')
volume = data.get('Volume')
close_1d = close.resample('D').last()
high_1d = high.resample('D').max()
low_1d = low.resample('D').min()

btc_close_1d = close_1d['BTC_USDT']
btc_ma50 = vbt.MA.run(btc_close_1d, 50)
ma20 = vbt.MA.run(close_1d, 20)
btc_bull_filter = btc_ma50.ma_below(btc_close_1d)

def entry_signal():
    #btc filter
    btc_bull_filter = btc_ma50.ma_below(btc_close_1d)
    #因为vbt会在一根bar的close处执行订单，为了避免lookahead bias需要把信号后移
    btc_bull_filter_at_23 = btc_bull_filter.shift(1, freq='23H') 
    btc_bull_filter_1h = btc_bull_filter_at_23.reindex(close.index, method='ffill')
    btc_bull_filter_1h.fillna(False, inplace=True)
    #trend indicator
    trend_entry = ma20.ma_crossed_below(close_1d) #收盘价上穿 ma20 时
    trend_entry.columns = trend_entry.columns.droplevel('ma_window')
    trend_entry_at_23 = trend_entry.shift(1, freq='23H') 
    trend_entry_1h = trend_entry_at_23.reindex(close.index, method='ffill')
    trend_entry_1h.fillna(False, inplace=True)
    #coin top20% filter
    coin_filter = pd.DataFrame(index=trend_entry_1h.index, columns=trend_entry_1h.columns)
    for _, row in df_filtered.iterrows():
        date = row['date']
        coin_pair = row['coin_pair']
        if coin_pair in coin_filter.columns:
            coin_filter.at[date, coin_pair] = True
    coin_filter_1h = coin_filter.groupby(coin_filter.index.date).ffill() #把1d信号填充到1h
    coin_filter_1h.fillna(False, inplace=True)
    coin_filter_1h = coin_filter_1h[:-1] #因为最底下莫名多出来一行

    mask = trend_entry_1h.vbt & coin_filter_1h
    mask_final = mask.vbt & btc_bull_filter_1h
    return mask_final

def exit_signal():
    #btc filter
    btc_bear_filter = btc_ma50.ma_above(btc_close_1d)
    btc_bear_filter_at_23 = btc_bear_filter.shift(1, freq='23H')
    btc_bear_filter_1h = btc_bear_filter_at_23.reindex(close.index, method='ffill')
    btc_bear_filter_1h.fillna(False, inplace=True)
    #trend indicator
    trend_exit = ma20.ma_crossed_above(close_1d) #收盘价下穿 ma20 时
    trend_exit.columns = trend_exit.columns.droplevel('ma_window')
    trend_exit_at_23 = trend_exit.shift(1, freq='23H')
    trend_exit_1h = trend_exit_at_23.reindex(close.index, method='ffill')
    trend_exit_1h.fillna(False, inplace=True)

    exit_mask_1h = trend_exit_1h.vbt | btc_bear_filter_1h
    return exit_mask_1h

def cal_atr():
    ATR = vbt.ATR.run(high_1d, low_1d, close_1d, window=14)
    atr = ATR.atr
    return atr

mask = entry_signal()
exit_mask = exit_signal()
atr_1d = cal_atr()
atr = atr_1d.reindex(mask.index, method='ffill')
    
entries = pd.DataFrame(False, index=mask.index, columns=mask.columns)
exits = pd.DataFrame(False, index=mask.index, columns=mask.columns)
holdings = {}
exited_coins = set()
size = pd.DataFrame(0, index=mask.index, columns=mask.columns)
capital_columns = ['Remaining Cash', 'Available Cash', 'Asset Value']
capital_df = pd.DataFrame(np.nan, index=mask.index, columns=capital_columns)
initial_cash = 10000
fees = 0.001
risk_factor = 0.01
position_count = 3
atr_window = 14  # ATR 的第一个索引
capital_df.iloc[0] = [initial_cash, initial_cash * 0.99, initial_cash]  # 初始资金分配

def update_capital_and_exit(date, coin_pair, exit_price, exit_size):
    exits.at[date, coin_pair] = True
    size.at[date, coin_pair] = -exit_size
    # print(coin_pair, "exit size",exit_size)
    
    capital_gain = exit_price * exit_size * (1 - fees)
    # total_sum_for_coin_pair = sum(trade['entry_price'] * trade['size'] for trade in holdings[coin_pair]['trades'])
    # profit = capital_gain - total_sum_for_coin_pair
    # print("该笔收益",date, coin_pair, profit)

    capital_df.at[date, 'Remaining Cash'] += capital_gain
    # print(coin_pair,"Remaining Cash",capital_df.at[date, 'Remaining Cash'])
    capital_df.at[date, 'Available Cash'] = capital_df.at[date, 'Remaining Cash'] * 0.99
    del holdings[coin_pair]

    asset_value_sum = sum(
        trade['entry_price'] * trade['size'] for coin in holdings for trade in holdings[coin]['trades']
    ) 
    # print("sum", asset_value_sum)

    capital_df.at[date, 'Asset Value'] = capital_df.at[date, 'Remaining Cash'] + asset_value_sum   
    exited_coins.add(coin_pair)
    print("Coins in holdings:", list(holdings.keys()))

def update_capital_and_entry(date, coin_pair, current_price, stake_amount, update_holdings=True):
    available_cash = capital_df.at[date, 'Available Cash']
    
    if stake_amount > available_cash:
        print(f"{coin_pair}: Skipped due to insufficient available cash.")
        return

    entries.at[date, coin_pair] = True
    trade_size = stake_amount / current_price
    size.at[date, coin_pair] = trade_size

    # 更新或添加新的交易记录到 holdings
    trade = {'entry_date': date, 'entry_price': current_price, 'size': trade_size}
    if update_holdings:
        holdings[coin_pair]['trades'].append(trade)
    else:
        holdings[coin_pair] = {'trades': [trade]}

    # 更新 capital_df
    capital_df.at[date, 'Remaining Cash'] -= trade_size * current_price * (1 + fees)
    capital_df.at[date, 'Available Cash'] = capital_df.at[date, 'Remaining Cash'] * 0.99

    # 计算新的 Asset Value
    asset_value = capital_df.at[date, 'Remaining Cash'] + sum(
        trade['entry_price'] * trade['size'] for coin in holdings for trade in holdings[coin]['trades']
    )
    capital_df.at[date, 'Asset Value'] = asset_value
    return True

for date, signals_on_date in mask.iterrows():  # 遍历 mask 中的每个日期

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
            exit_size = sum(trade['size'] for trade in holdings[coin_pair]['trades'])
            update_capital_and_exit(date, coin_pair, exit_price, exit_size)
            print(coin_pair, "update exit size cas force",capital_df.loc[date])
        continue

    for coin_pair in list(holdings):  # 使用list来避免在循环中修改字典
        entry_date = holdings[coin_pair]['trades'][0]['entry_date']
        exit_size = sum(trade['size'] for trade in holdings[coin_pair]['trades'])
        current_price = close.at[date, coin_pair]
        atr_value = atr.loc[date, (atr_window, coin_pair)]
        last_entry_price = holdings[coin_pair]['trades'][-1]['entry_price']
        entry_price = sum(trade['entry_price'] * trade['size'] for trade in holdings[coin_pair]['trades']) / exit_size

        if current_price <= 0.5 * entry_price:
            update_capital_and_exit(date, coin_pair, current_price, exit_size)
            print(coin_pair, "update exit size cas stoploss",capital_df.loc[date])          

        elif exit_mask.at[date, coin_pair]:
            update_capital_and_exit(date, coin_pair, current_price, exit_size)
            print(coin_pair, "update exit size cas exit trend",capital_df.loc[date])                   

        elif (current_price >= last_entry_price + 0.25 * atr_value) and (1 <= len(holdings[coin_pair]['trades']) <= (position_count-1)): #加仓最多3次
            first_trade = holdings[coin_pair]['trades'][0]
            stake_amount = first_trade['entry_price'] * first_trade['size']
            result = update_capital_and_entry(date, coin_pair, current_price, stake_amount, update_holdings=True)
            if result:
                print(coin_pair, "adjustment entry size",capital_df.loc[date])

    signals = signals_on_date[signals_on_date].index.tolist()  # 检查当前日期有哪些币对发出了入场信号
    if not signals or len(holdings) >= 10:
        continue
    date_daily = pd.to_datetime(date).normalize() # 将 mask 的按小时日期转换为按天日期，以便与 df_filtered 对齐
    rank_on_date = df_filtered[df_filtered['date'] == date_daily]
    rank_on_date = rank_on_date[rank_on_date['coin_pair'].isin(signals)]
    sorted_signals = rank_on_date.sort_values('rank')['coin_pair'].tolist()

    for coin_pair in sorted_signals:

        if len(holdings) < 10 and coin_pair not in holdings and coin_pair not in exited_coins:
            # 计算资金量
            asset_value = capital_df.at[date, 'Asset Value']
            entry_price = close.at[date, coin_pair]
            atr_value = atr.loc[date, (atr_window, coin_pair)]
            stake_amount = asset_value * risk_factor * entry_price / (atr_value * position_count)
            # print(coin_pair,"stake amount",stake_amount,"asset",asset_value, "price",entry_price)
            result = update_capital_and_entry(date, coin_pair, entry_price, stake_amount, update_holdings=False)
            if result:
                print(coin_pair, "update entry size",capital_df.loc[date])
            # print(json.dumps(holdings, indent=4, cls=CustomEncoder))
exits = exits.fillna(False)
capital_df.to_csv('capital_data.csv', index=True)

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

qs.reports.html(daily_returns, benchmark=btc_returns, output='report_trend.html')

orders = pf.orders.records_readable
csv_path = 'orders_trend.csv'
orders.to_csv(csv_path)
print("csv done")

def gen_tradelog(csv_path):
    orders_df = pd.read_csv(csv_path)
    trades = []

    buy_counter = 0
    trade = {}
    accumulated_fees = 0

    for i, row in orders_df.iterrows():
        if row['Side'] == 'Buy':
            if buy_counter == 0:
                trade = {
                    '交易币对': row['Column'],
                    '首次买入时间': row['Index'],
                    '首次买入价格': row['Price'],
                    '首次买入数量': row['Size']
                }
            else:
                trade[f'加仓{buy_counter}买入价格'] = row['Price']
                trade[f'加仓{buy_counter}买入数量'] = row['Size']
            accumulated_fees += row['Fees'] 
            buy_counter += 1

        elif row['Side'] == 'Sell':
            trade['卖出时间'] = row['Index']
            trade['卖出价格'] = row['Price']
            trade['卖出数量'] = row['Size']
            trade['USDT Value'] = trade.get('首次买入价格', 0) * trade.get('首次买入数量', 0)
            trade['USDT Value'] += sum([trade.get(f'加仓{i}买入价格', 0) * trade.get(f'加仓{i}买入数量', 0) for i in range(1, buy_counter)])  
            accumulated_fees += row['Fees']
            trade['Fees'] = accumulated_fees
            trades.append(trade)
            buy_counter = 0
            trade = {}
            accumulated_fees = 0 

    columns_order = [
        '交易币对', '首次买入时间', '首次买入价格', '首次买入数量',
        '加仓1买入价格', '加仓1买入数量', '加仓2买入价格', '加仓2买入数量',
        '卖出时间', '卖出价格', '卖出数量', 'USDT Value', 'Fees'
    ]

    # 创建 DataFrame 时使用定义的列顺序
    tradelog_df = pd.DataFrame(trades, columns=columns_order)
    tradelog_df.sort_values(by='首次买入时间', inplace=True)  # 确保按照首次买入时间排序
    initial_capital = 10000
    total_capital = initial_capital
    pnls = []
    total_capitals = []
    single_trade_returns = []

    for index, row in tradelog_df.iterrows():
        pnl = (row['卖出价格'] * row['卖出数量']) - row['USDT Value'] - row['Fees']  # 按新公式计算 PnL
        pnls.append(pnl)
        total_capital += pnl
        total_capitals.append(total_capital)

        single_trade_return = (pnl / row['USDT Value']) * 100 if row['USDT Value'] != 0 else 0
        single_trade_returns.append(single_trade_return)

    tradelog_df['PnL'] = pnls
    tradelog_df['总资金'] = total_capitals
    tradelog_df['PnL Ratio'] = (tradelog_df['PnL'] / tradelog_df['总资金']) * 100
    tradelog_df['单笔收益率'] = single_trade_returns

    tradelog_df['PnL'] = tradelog_df['PnL'].apply(lambda x: f"{x:.2f}")
    tradelog_df['总资金'] = tradelog_df['总资金'].apply(lambda x: f"{x:.2f}")
    tradelog_df['PnL Ratio'] = tradelog_df['PnL Ratio'].apply(lambda x: f"{x:.2f}%")
    tradelog_df['单笔收益率'] = tradelog_df['单笔收益率'].apply(lambda x: f"{x:.2f}%")

    tradelog_csv_path = 'tradelog_trend.csv'  
    tradelog_df.to_csv(tradelog_csv_path, index=False)

    print("Tradelog CSV has been generated successfully.")

gen_tradelog(csv_path)
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")










