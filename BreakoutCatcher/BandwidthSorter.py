import logging
from typing import Any, Dict, List
import numpy as np
from datetime import datetime, timedelta
import time
import talib.abstract as ta
from freqtrade.constants import Config, ListPairsWithTimeframes
from freqtrade.exceptions import OperationalException
from freqtrade.exchange.types import Tickers
from freqtrade.misc import plural
from freqtrade.plugins.pairlist.IPairList import IPairList, PairlistParameter
from freqtrade.util import dt_floor_day, dt_now, dt_ts

logger = logging.getLogger(__name__)

class BandwidthSorter(IPairList):
    """
    Filters pairs based on Bollinger Bands Width over the past 20 days
    and sorts them from smallest to largest width.
    """

    def __init__(self, exchange, pairlistmanager,
                 config: Dict[str, Any], pairlistconfig: Dict[str, Any],
                 pairlist_pos: int) -> None:
        super().__init__(exchange, pairlistmanager, config, pairlistconfig, pairlist_pos)

        # SMA period for Bollinger Bands calculation
        self._sma_period = pairlistconfig.get('sma_period', 20)
        # Number of standard deviations for Bollinger Bands calculation
        self._std_devs = pairlistconfig.get('std_devs', 2.0)
        self._refresh_period = pairlistconfig.get('refresh_period', 1800)

        if not exchange.exchange_has('fetchOHLCV'):
            raise OperationalException(
                'Exchange does not support OHLCV data. '
                'Required for Bollinger Bands calculation in BollingerBandsFilter.'
            )

    @property
    def needstickers(self) -> bool:
        """
        Boolean property defining if tickers are necessary.
        We require OHLCV data, not tickers, so this is False.
        """
        return False

    def short_desc(self) -> str:
        """
        Short whitelist method description - used for startup-messages.
        """
        return f"{self.name} - Sorting pairs based on {self._sma_period}-day Bollinger Bands Width."

    @staticmethod
    def description() -> str:
        """
        Detailed description of the method.
        """
        return "Sorts pairs based on Bollinger Bands Width calculated over a configurable SMA period."

    @staticmethod
    def available_parameters() -> Dict[str, PairlistParameter]:
        return {
            "sma_period": {
                "type": "number",
                "default": 20,
                "description": "SMA period for Bollinger Bands calculation.",
                "help": "The period to consider for calculating the SMA in Bollinger Bands."
            },
            "std_devs": {
                "type": "number",
                "default": 2,
                "description": "Number of standard deviations for Bollinger Bands calculation.",
                "help": "The number of standard deviations to consider for calculating the Bollinger Bands."
            },
            **IPairList.refresh_period_parameter()
        }

    def filter_pairlist(self, pairlist: List[str], tickers: Tickers) -> List[str]:
        """
        Filters and sorts pairlist by Bollinger Bands Width and returns the sorted list.
        :param pairlist: pairlist to filter or sort.
        :param tickers: Ignored as we don't need tickers.
        :return: new pairlist, sorted by Bollinger Bands Width.
        """
        bb_width_pairs = []

        now = datetime.now()
        since = now - timedelta(days=(self._sma_period * 2) + 1)
        since_ms = int(time.mktime(since.timetuple()) * 1000)

        for pair in pairlist:
            ohlcv = self._exchange.get_historic_ohlcv(pair, timeframe='1d', since_ms=since_ms, candle_type='')
            ohlcv = ohlcv[:-1]

            if ohlcv is None or len(ohlcv) < self._sma_period:
                self.log_once(f"Insufficient data for {pair}. Only {len(ohlcv) if ohlcv else 0} candles found.",
                              logger.info)
                continue

            close_prices = np.array([x[4] for x in ohlcv])
            upperband, middleband, lowerband = ta.BBANDS(close_prices, timeperiod=self._sma_period, nbdevup=self._std_devs, nbdevdn=self._std_devs, matype=0)
            if middleband[-1] == 0:
                self.log_once(f"Middle band is zero for {pair}, skipping.", logger.info)
                continue    
            # print(pair,upperband)
            bb_width = (upperband[-1] - lowerband[-1]) / middleband[-1]

            bb_width_pairs.append((pair, bb_width))

        bb_width_pairs.sort(key=lambda x: x[1])  # Sort from smallest to largest width
        # print(bb_width_pairs)
        selected_pairs = [pair for pair, _ in bb_width_pairs]

        self.log_once(f"Original pairlist: {pairlist}", logger.info)
        self.log_once(f"Pairs after sorting by Bollinger Bands Width: {selected_pairs}", logger.info)

        return selected_pairs