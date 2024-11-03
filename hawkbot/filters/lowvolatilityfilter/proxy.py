from hawkbot.filters.lowvolatilityfilter.low_volatility_filter import LowVolatilityFilter

def get_filter_class(name: str):
    if name != LowVolatilityFilter.__name__:
        raise Exception('Different value requested')
    return LowVolatilityFilter
