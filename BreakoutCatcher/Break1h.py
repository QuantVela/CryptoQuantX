# pragma pylint: disable=missing-docstring, invalid-name, pointless-string-statement
# flake8: noqa: F401
# isort: skip_file
# --- Do not remove these libs ---
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, List

from freqtrade.strategy import (BooleanParameter, CategoricalParameter, DecimalParameter,
                                IntParameter, IStrategy, merge_informative_pair, informative)
from freqtrade.persistence import Trade
# --------------------------------
# Add your lib to import here
import talib.abstract as ta
import pandas_ta as pta
from technical import qtpylib
import logging

# 策略规则：
# 币对筛选：3 日累计成交量前 36 个，去除稳定币和 BTC
# 进场：突破 30 日内最高价，且 BTC > MA50
# 出场：BTC < MA50 或第三天收盘时
# 资金均分 10 份，最多持仓 10 个币

logger = logging.getLogger(__name__)

class Break1h(IStrategy):

    # Strategy interface version - allow new iterations of the strategy interface.
    INTERFACE_VERSION = 3

    # Optimal timeframe for the strategy.
    timeframe = '1h'

    # Can this strategy go short?
    can_short: bool = False

    # Minimal ROI designed for the strategy.
    # This attribute will be overridden if the config file contains "minimal_roi".
    minimal_roi = {}

    # Optimal stoploss designed for the strategy.
    # This attribute will be overridden if the config file contains "stoploss".
    stoploss = -0.50

    # Trailing stoploss
    trailing_stop = False
    # trailing_only_offset_is_reached = False
    # trailing_stop_positive = 0.01
    # trailing_stop_positive_offset = 0.0  # Disabled / not configured 

    # Run "populate_indicators()" only for new candle.
    process_only_new_candles = True

    # These values can be overridden in the config.
    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    # Number of candles the strategy requires before producing valid signals
    startup_candle_count: int = 800

    # Strategy parameters
    # buy_rsi = IntParameter(10, 40, default=30, space="buy")
    # sell_rsi = IntParameter(60, 90, default=70, space="sell")

    # Optional order type mapping.
    order_types = {
        'entry': 'limit',
        'exit': 'limit',
        'stoploss': 'market',
        "emergency_exit": "market",
        "force_entry": "market",
        "force_exit": "market",
        'stoploss_on_exchange': False
    }

    # Optional order time in force.
    order_time_in_force = {
        'entry': 'GTC',
        'exit': 'GTC'
    }
    
    @property
    def plot_config(self):
        return {
            'main_plot': {
                'high_30': {'color': '#E8E5A9'},
                'low_10': {'color': '#CA9C6E'}
            },
            'subplots': {
                "ATR": {
                    'atr': {'color': '#BA9FF1'},  # 平均真实范围
                },
            }
        }
    def informative_pairs(self):
        pairs = self.dp.current_whitelist()
        informative_pairs = [(pair, '1d') for pair in pairs]
        informative_pairs += [("BTC/USDT", "1d")]
        return informative_pairs
    
    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:   
        if not self.dp:
            # Don't do anything if DataProvider is not available.
            return dataframe
        
        # dataframe['tr'] = ta.TRANGE(dataframe) # 计算真实波动幅度（TR）
        # dataframe['atr'] = ta.ATR(dataframe, timeperiod=10*24) # 计算 ATR
        dataframe['high_30'] = dataframe['high'].rolling(window=30*24).max()
        dataframe['low_10'] = dataframe['low'].rolling(window=10*24).min()

        # 为BTC计算MA50
        btc_inf_tf = '1d'
        btc_informative = self.dp.get_pair_dataframe(pair="BTC/USDT", timeframe=btc_inf_tf) #close_1d_x
        btc_informative['btc_ma50'] = ta.SMA(btc_informative, timeperiod=50) #btc_ma50_1d
        dataframe = merge_informative_pair(dataframe, btc_informative, self.timeframe, btc_inf_tf, ffill=True)

        pair_inf_tf = '1d'
        pair_informative = self.dp.get_pair_dataframe(pair=metadata['pair'], timeframe=pair_inf_tf) #close_1d_y
        pair_informative[f'{metadata["pair"].replace("/", "_")}_atr'] = ta.ATR(pair_informative, timeperiod=10) #SOL_USDT_atr_1d

        # Merge the specified pair informative
        dataframe = merge_informative_pair(dataframe, pair_informative, self.timeframe, pair_inf_tf, ffill=True)
        # pd.set_option('display.max_columns', None)
        # print(dataframe.iloc[-30:])
    
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:

        dataframe.loc[
            (
                (dataframe['close_1d_x'] > dataframe['btc_ma50_1d']) & #BTC>MA50
                (dataframe['high'] >= dataframe['high_30'])
            ),
            'enter_long'] = 1

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:

        dataframe.loc[
            (
                (qtpylib.crossed_below(dataframe['close_1d_x'], dataframe['btc_ma50_1d']))#BTC<MA50
                # (dataframe['close_1d_x'] < dataframe['btc_ma50_1d'])  #BTC<MA50
            ),
            ['exit_long', 'exit_tag']
        ] = (1, 'exit_btc_below_ma50')

        return dataframe

    def custom_exit(self, pair: str, trade: 'Trade', current_time: 'datetime', current_rate: float,
                    current_profit: float, **kwargs):
        # dataframe, _ = self.dp.get_analyzed_dataframe(pair=pair, timeframe=self.timeframe)
        # last_candle = dataframe.iloc[-1].squeeze()
        # low_10 = last_candle['low_10']
        # logger.info(f'pair{pair} days {(current_time - trade.open_date_utc).days}')
        if (current_time - trade.open_date_utc).days > 1:           
            if current_time.hour == 0:
                logger.info(f"pair{pair} exit because days {(current_time - trade.open_date_utc).days} hour: {current_time.hour}")
                return 'exit_after_3_days'
        # if current_rate <= low_10:
        #     logger.info(f'current_rate{current_rate}<=low_10{low_10}')
        #     return 'exit_low_10'

    # position_adjustment_enable = True
    # max_entry_position_adjustment = 2
                         
    # def custom_stake_amount(self, pair: str, current_time: datetime, current_rate: float,
    #                             proposed_stake: float, min_stake: Optional[float], max_stake: float,
    #                             leverage: float, entry_tag: Optional[str], side: str,
    #                             **kwargs) -> float:
    #     dataframe, _ = self.dp.get_analyzed_dataframe(pair=pair, timeframe=self.timeframe)
    #     current_candle = dataframe.iloc[-1].squeeze()      
    #     atr = current_candle[f'{pair.replace("/", "_")}_atr_1d']
 
    #     risk_factor = 0.01
    #     total_wallet_balance = self.wallets.get_total_stake_amount()
    #     # 每次交易使用总资金的 33%
    #     position_size = (total_wallet_balance * risk_factor * current_rate) / (atr * (self.max_entry_position_adjustment + 1))
    #     logger.info(f'{pair} position_size = total amount{total_wallet_balance}*{risk_factor}*current rate{current_rate}/(atr{atr}*3) = {position_size}')
    #     return position_size

    # def adjust_trade_position(self, trade: Trade, current_time: datetime,
    #                             current_rate: float, current_profit: float,
    #                             min_stake: Optional[float], max_stake: float,
    #                             current_entry_rate: float, current_exit_rate: float,
    #                             current_entry_profit: float, current_exit_profit: float,
    #                             **kwargs) -> Optional[float]:
    #     dataframe, _ = self.dp.get_analyzed_dataframe(pair=trade.pair, timeframe=self.timeframe)
    #     current_candle = dataframe.iloc[-1].squeeze()
    #     atr = current_candle[f'{trade.pair.replace("/", "_")}_atr_1d']
    #     if trade.trade_direction == "long":
    #         # 获取已成交的入场订单
    #         filled_entries = trade.select_filled_orders(trade.entry_side)
    #         count_of_entries = trade.nr_of_successful_entries
    #         try:
    #             stake_amount = filled_entries[0].stake_amount
    #             last_order_price = filled_entries[-1].average
    #             # logger.info(f'{trade.pair} current_rate={current_rate}, price={last_order_price + 0.5 * atr}')
    #             # 如果当前价格大于或等于最后一次订单价格加上 1/2 ATR，买入相同数量的stake_amount
    #             if current_rate >= last_order_price + 0.5 * atr:
    #                 logger.info(f'{trade.pair} add position size={stake_amount}, atr={atr}, price={last_order_price + 0.5 * atr}')
    #                 return stake_amount
    #         except Exception as exception:
    #             return None
        # if trade.trade_direction == "short":
        #     # 获取已成交的入场订单
        #     filled_entries = trade.select_filled_orders(trade.entry_side)
        #     count_of_entries = trade.nr_of_successful_entries
        #     try:
        #         stake_amount = filled_entries[0].stake_amount
        #         last_order_price = filled_entries[-1].average
        #         # 如果当前价格小于或等于最后一次订单价格减去 1/2 ATR，加仓相同数量的stake_amount
        #         if current_rate <= last_order_price - 0.5 * atr:
        #             return stake_amount
        #     except Exception as exception:
        #         return None        
        return None

    # use_custom_stoploss = True

    # def custom_stoploss(self, pair: str, trade: 'Trade', current_time: datetime,
    #                     current_rate: float, current_profit: float, after_fill: bool,
    #                     **kwargs) -> Optional[float]:
    #     dataframe, _ = self.dp.get_analyzed_dataframe(pair, self.timeframe)
    #     current_candle = dataframe.iloc[-1].squeeze()
    #     atr = current_candle[f'{pair.replace("/", "_")}_atr_1d']

    #     if trade.trade_direction == "long":
    #         filled_entries = trade.select_filled_orders(trade.entry_side)
    #         # count_of_entries = trade.nr_of_successful_entries
    #         try:
    #             last_order_price = filled_entries[-1].average
    #             stoploss_price = last_order_price - atr * 2 #To do:回测看下atr1和 2 哪个更好，海龟是 2
    #             average_entry_price = trade.open_rate
    #             stoploss_percentage = -round((average_entry_price - stoploss_price) / average_entry_price, 4)
    #             logger.info(f'{pair} stoploss_price{stoploss_price} = last_order_price{last_order_price} - atr{atr}*2, stoploss_percentage{stoploss_percentage}')
    #             return stoploss_percentage
    #         except Exception as exception:
    #             return None
    #     return None