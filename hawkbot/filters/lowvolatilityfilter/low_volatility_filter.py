import logging
import pdb
from typing import List, Dict

from hawkbot.core.candlestore.candlestore import Candlestore
from hawkbot.core.data_classes import SymbolPositionSide, Timeframe, ExchangeState, FilterResult
from hawkbot.core.model import PositionSide
from hawkbot.exceptions import InvalidConfigurationException
from hawkbot.exchange.exchange import Exchange
from hawkbot.core.filters.filter import Filter
from hawkbot.utils import get_percentage_difference, readable

logger = logging.getLogger(__name__)


class LowVolatilityFilter(Filter):
    @classmethod
    def filter_name(cls):
        return cls.__name__

    def __init__(self, bot, name: str, filter_config, redis_host: str, redis_port: int):
        super().__init__(bot=bot, name=name, filter_config=filter_config, redis_host=redis_host, redis_port=redis_port)
        self.exchange: Exchange = None  # Injected by framework
        self.exchange_state: ExchangeState = None  # Injected by framework
        self.candle_store: Candlestore = None  # Injected by framework
        self.reference_timeframe: Timeframe = None
        self.reference_candle_nr: int = None
        self.sort = None
        self.max_threshold = None


        if 'sort' in self.filter_config:
            self.sort = filter_config['sort']
            if self.sort not in ['asc', 'desc']:
                raise InvalidConfigurationException("If specified, the parameter 'sort' needs to be specified as either 'asc' or 'desc' for the LowVolatilityFilter")

        self.top = None
        if 'top' in self.filter_config:
            self.top = filter_config['top']

        if 'max_threshold' in self.filter_config:
            self.max_threshold = filter_config['max_threshold']
            if self.max_threshold <= 0:
                raise InvalidConfigurationException(f"The parameter 'max_threshold' contains a value of "
                                                    f"{self.max_threshold} which is not supported. If specified, "
                                                    f"the value must be greater than 0.")

        if 'reference_timeframe' not in self.filter_config:
            raise InvalidConfigurationException("VolatilityFilter configuration is missing the mandatory parameter "
                                                "'reference_timeframe'")
        else:
            self.reference_timeframe = Timeframe.parse(self.filter_config['reference_timeframe'])

        if 'reference_candle_nr' not in self.filter_config:
            raise InvalidConfigurationException("VolatilityFilter configuration is missing the mandatory parameter "
                                                "'reference_candle_nr'")
        else:
            self.reference_candle_nr = int(self.filter_config['reference_candle_nr'])

        if self.reference_candle_nr <= 0:
            raise InvalidConfigurationException("The parameter 'reference_candle_nr' for the VolatilityFilter needs to "
                                                "be greater than 0")

    def filter_symbols(self,
                       starting_list: List[str],
                       first_filter: bool,
                       previous_filter_results: List[FilterResult]) -> Dict[SymbolPositionSide, Dict]:
        non_volatile_symbols = {}

        logger.debug(f"initial symbols {len(starting_list)} symbols: {starting_list}")
        logger.debug(f"Config: {self.filter_config}")
        for symbol in starting_list:
            reference_candles = self.candle_store.get_last_candles(symbol=symbol,
                                                                   timeframe=self.reference_timeframe,
                                                                   amount=self.reference_candle_nr)
            logger.debug(
                f"Candles {reference_candles} for symbol {symbol} and timeframe {self.reference_timeframe.name}")
            last_candle_close_date = max([candle.close_date for candle in reference_candles])
            last_candle_close_date = readable(last_candle_close_date)
            lowest_low = min([candle.low for candle in reference_candles])
            highest_high = max([candle.high for candle in reference_candles])
            price_ratio_change = get_percentage_difference(lowest_low, highest_high)
            logger.warning(f"symbols {symbol} has price_ratio_change {price_ratio_change}, lowest_low {lowest_low}, highest_high {highest_high}")
            if price_ratio_change > 0:
                # pdb.set_trace()
                if self.max_threshold is not None:
                    if price_ratio_change <= self.max_threshold:
                        logger.debug(f"{symbol} : ADDING {symbol} to volatile symbols for potential "
                                    f"entry, price changed {price_ratio_change:.3f}% between lowest low {lowest_low} and {highest_high} in the "
                                    f"past {self.reference_candle_nr} candles of {self.reference_timeframe.name} "
                                    f"(last close: {last_candle_close_date})")
                        non_volatile_symbols[symbol] = {
                            'price_ratio_change': price_ratio_change,
                            'lowest_low': lowest_low,
                            'highest_high': highest_high
                        }
                    else:
                        logger.debug(f"{symbol} : NOT ADDING {symbol} to volatile symbols for potential "
                                    f"entry, price changed {price_ratio_change:.3f}% between lowest low {lowest_low} and {highest_high} in the "
                                    f"past {self.reference_candle_nr} candles of {self.reference_timeframe.name} due to exceeded volatility threshold"
                                    f"(last close: {last_candle_close_date})")
            else:
                logger.debug(f"{symbol} : NOT ADDING {symbol} to volatile symbols for potential "
                            f"entry, price did not change between lowest low {lowest_low} and {highest_high} in the "
                            f"past {self.reference_candle_nr} candles of {self.reference_timeframe.name} "
                            f"(last close: {last_candle_close_date})")

        sort_descending = self.sort is None or self.sort == 'desc'
        sorted_symbols = sorted(non_volatile_symbols.items(), key=lambda x: x[1]['price_ratio_change'], reverse=sort_descending)
        logger.debug(f"sorted_symbols {len(sorted_symbols)} symbols: {sorted_symbols}")
        if self.top is not None:
            sorted_symbols = sorted_symbols[:self.top]
        logger.debug(f"non volatile symbols {len(sorted_symbols)} symbols: {list(sorted_symbols)}")

        filtered_symbols = {}
        for symbol, data in sorted_symbols:
            if symbol in starting_list:
                logger.debug(f"{symbol}: Allowing symbol based on volatility setting {data}")
                filtered_symbols[symbol] = {}
            else:
                logger.debug(f"{symbol}: Discarding symbol because it's not in starting list")


        logger.info(f"non volatile filtered symbols {len(filtered_symbols)} symbols: {filtered_symbols}")
        return filtered_symbols
