import pandas as pd
import matplotlib.pyplot as plt
from hdq_utils import getHdqPbAsDF
from metadata import metadata
from constants import CURVE_ID, OBSERVATION_DATE, CONTRACT_START, VALUE, EXPIRY_DATE, DAYS_FROM_EXPIRY, hdq_expression


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


def enrich_ts(df, metadata_df):
    return pd.merge(df, metadata_df, left_on=CURVE_ID, right_index=True)


def calc_days_from_expiry(df):
    df[DAYS_FROM_EXPIRY] = df.index.get_level_values(OBSERVATION_DATE) - df[EXPIRY_DATE]
    return df


def reset_index_for_chart_data(df, index_columns):
    # we always need the curve_id
    index_columns.append(CURVE_ID)
    df.reset_index(inplace=True)
    df.set_index(index_columns, drop=True, inplace=True)
    return df[VALUE]


def pivot_for_chart_data(s, chart_index):
    df = s.to_frame()
    df.reset_index(inplace=True)
    df = df.pivot(index=chart_index, columns=CURVE_ID)
    # the value column name is added to the column index
    # remove it here
    df.columns = df.columns.droplevel(None)
    return df


if __name__ == '__main__':
    symbols = ['IPEBRT19Z', 'IPEBRT20Z', 'IPEBRT21Z']

    df_expiries = get_hdq_expiries(symbols=symbols)

    df_ts = build_ts_df(symbols=symbols, metadata=metadata)

    # enrich with expiry dates
    df_ts_enrich = enrich_ts(df_ts, df_expiries)

    # calculate days from expiry
    df = calc_days_from_expiry(df_ts_enrich)

    # make days_from_expiry the new index
    df = reset_index_for_chart_data(df, [DAYS_FROM_EXPIRY])

    # pivot the data ready to chart it
    df = pivot_for_chart_data(df, chart_index=DAYS_FROM_EXPIRY)

    df.plot(kind='line')
    plt.show()
    print('great')

