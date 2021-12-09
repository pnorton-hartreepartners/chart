import datetime as dt
import pandas as pd
from constants import OBSERVATION_DATE, CONTRACT_START, EXPIRY_DATE, CURVE_ID, SYMBOL, SYMBOLS, VALUE, \
    LONG_SCREEN_NAME, CURRENCY, UOM, PROCESSOR, hdq_expression
from hdq_utils import getHdqPbAsDF


class HdqProcessor:
    def __init__(self):
        self.curve_metadata = curve_metadata
        self.symbol_metadata = symbol_metadata
        self.df_curve = pd.DataFrame()
        self.df_expiries = pd.DataFrame()
        self.curve_id = None

    def build_single_curve_df(self, curve_id):
        self.curve_id = curve_id
        for symbol in self.curve_metadata[self.curve_id][SYMBOLS]:
            # call hdq api
            df = self._call_hdq_api_for_timeseries(symbol)
            # rename columns and set index
            df = self._clean_hdq_timeseries(df, symbol)
            self.df_curve = pd.concat([self.df_curve, df], axis='rows')
        # add the curve_id as a column then move to the index
        self.df_curve[CURVE_ID] = self.curve_id
        self.df_curve.set_index(CURVE_ID, drop=True, append=True, inplace=True)
        # and reorder
        self.df_curve = self.df_curve.reorder_levels([CURVE_ID, CONTRACT_START, OBSERVATION_DATE], axis='index')

    def get_expiries_for_selected_curve(self):
        for symbol in self.curve_metadata[self.curve_id][SYMBOLS]:
            df = self._call_hdq_api_for_expiry(symbol)
            df = self._clean_hdq_expiry(df, symbol=symbol)
            # add the contract start date from our metadata
            df[CONTRACT_START] = symbol_metadata[symbol][CONTRACT_START]
            # add the curve_id
            df[CURVE_ID] = self.curve_id
            # set the index
            df.set_index([CURVE_ID, CONTRACT_START], drop=True, append=True, inplace=True)
            # now we can remove the symbol
            df.index = df.index.droplevel(SYMBOL)
            # stack the df from each symbol
            self.df_expiries = pd.concat([self.df_expiries, df], axis='rows')
        # and reorder
        self.df_expiries = self.df_expiries.reorder_levels([CURVE_ID, CONTRACT_START], axis='index')

    def _clean_hdq_timeseries(self, df, symbol):
        # date
        df.rename(columns={'Date': OBSERVATION_DATE}, inplace=True)
        # set the index here so the remaining column is definitely the value
        df.set_index(OBSERVATION_DATE, drop=True, inplace=True)
        # value
        df.columns = [VALUE]
        # add the contract start date from our metadata
        df[CONTRACT_START] = self.symbol_metadata[symbol][CONTRACT_START]
        # and set the index again
        df.set_index(CONTRACT_START, drop=True, append=True, inplace=True)
        return df

    @staticmethod
    def _clean_hdq_expiry(df, symbol):
        df.columns = [EXPIRY_DATE]
        df.index = [symbol]
        df.index.name = SYMBOL
        return df

    @staticmethod
    def _call_hdq_api_for_timeseries(symbol):
        kwargs = {'method': 'TimeSeries',
                  'symbol': symbol}
        return getHdqPbAsDF(expression=hdq_expression.format(**kwargs))

    @staticmethod
    def _call_hdq_api_for_expiry(symbol):
        kwargs = {'method': 'FutureExpiry',
                  'symbol': symbol}
        return getHdqPbAsDF(expression=hdq_expression.format(**kwargs))


symbol_metadata = {
    'IPEBRT19Z': {CONTRACT_START: dt.date(2019, 12, 1)},
    'IPEBRT20Z': {CONTRACT_START: dt.date(2020, 12, 1)},
    'IPEBRT21Z': {CONTRACT_START: dt.date(2021, 12, 1)}
}

curve_metadata = {
    1000000: {
        'symbols': ['IPEBRT19Z', 'IPEBRT20Z', 'IPEBRT21Z'],
        'names': {LONG_SCREEN_NAME: 'IPE Brent Futures'},
        PROCESSOR: HdqProcessor,
        UOM: 'bbl',
        CURRENCY: 'USD'
    },
    1000001: {
        'symbols': ['NAPphCIFNWE'],
        'names': {LONG_SCREEN_NAME: 'Naphtha curve'},
        PROCESSOR: HdqProcessor,
        UOM: 'MT',
        CURRENCY: 'USD'
    },
}
