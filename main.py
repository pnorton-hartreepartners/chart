import pandas as pd
from hdq_utils import getHdqPbAsDF
from metadata import metadata
from constants import CURVE_ID, OBSERVATION_DATE, CONTRACT_START, VALUE, hdq_expression


config_rbob_ebob_builtup_arb = [
    ["Product", "Def code", "Factor", "Period", "Qty"],
    ["RBOB Cal Swap", "NYMRBOBCalSwap", 1.0000, 1.00, 100.00],
    ["EBOB (Argus Gas FOB ARA)", "ArgGASFBARA", 0.0029, 1.00, -65.00],
    ["Naphtha CIF NWE", "NAPphCIFNWE", 0.0027, 1.00, -35.00],
    ["TC2 $/ton", "TC2_USDMT_", 0.0032, 1.00, -100.00],
    ["RINRVOCost", "RINRVOCost", 1.0000, 1.00, -100.00]]


def clean_hdq_timeseries(df, symbol):
    # date
    df.rename(columns={'Date': OBSERVATION_DATE}, inplace=True)
    df.set_index([OBSERVATION_DATE], drop=True, inplace=True)
    # value
    df.columns = [VALUE]
    # curve_id
    df[CURVE_ID] = symbol
    df.set_index(CURVE_ID, drop=True, append=True, inplace=True)
    return df


def add_forward_date_to_hdq_timeseries(df, metadata_df):
    df = df.merge(metadata_df[CONTRACT_START], how='inner', left_on=CURVE_ID, right_index=True)
    df.set_index(CONTRACT_START, drop=True, append=True, inplace=True)
    return df


def clean_hdq_expiry(df, symbol):
    df.columns = ['expiry_date']
    df.index = [symbol]
    return df


def build_ts_df(symbols, metadata):
    metadata_df = pd.DataFrame.from_dict(metadata, orient='index')
    df_ts = pd.DataFrame()
    for symbol in symbols:
        method = 'TimeSeries'
        kwargs = {'method': method,
                  'symbol': symbol}
        df = getHdqPbAsDF(expression=hdq_expression.format(**kwargs))
        df = clean_hdq_timeseries(df, symbol)
        df = add_forward_date_to_hdq_timeseries(df, metadata_df)
        df_ts = pd.concat([df_ts, df], axis='rows')
    return df_ts


def get_hdq_expiries(symbols):
    df_expiries = pd.DataFrame()
    for symbol in symbols:
        method = 'FutureExpiry'
        kwargs = {'method': method,
                  'symbol': symbol}
        df = getHdqPbAsDF(expression=hdq_expression.format(**kwargs))
        df = clean_hdq_expiry(df, symbol=symbol)
        df_expiries = pd.concat([df_expiries, df], axis='rows')
    return df_expiries


if __name__ == '__main__':
    print('PyCharm')
    symbols = ['IPEBRT19Z', 'IPEBRT20Z', 'IPEBRT21Z']
    methods = ['TimeSeries', 'FutureExpiry']

    df_expiries = get_hdq_expiries(symbols=['IPEBRT19Z', 'IPEBRT20Z', 'IPEBRT21Z'])

    df_ts = build_ts_df(symbols=['IPEBRT19Z', 'IPEBRT20Z', 'IPEBRT21Z'], metadata=metadata)

    print(df_ts)
    print(df_expiries)



