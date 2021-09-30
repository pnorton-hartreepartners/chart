import pandas as pd
import matplotlib.pyplot as plt
from hdq_utils import getHdqPbAsDF
from metadata import symbol_metadata, curve_metadata
from constants import CURVE_ID, OBSERVATION_DATE, CONTRACT_START, VALUE, EXPIRY_DATE, DAYS_FROM_EXPIRY, SYMBOLS, SYMBOL, \
    LEGEND, hdq_expression


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
    # symbol
    df[SYMBOL] = symbol
    df.set_index(SYMBOL, drop=True, append=True, inplace=True)
    return df


def add_forward_date_to_hdq_timeseries(df, metadata_df):
    df = df.merge(metadata_df[CONTRACT_START], how='inner', left_on=CURVE_ID, right_index=True)
    df.set_index(CONTRACT_START, drop=True, append=True, inplace=True)
    return df


def clean_hdq_expiry(df, symbol):
    df.columns = [EXPIRY_DATE]
    df.index = [symbol]
    df.index.name = SYMBOL
    return df


def build_single_curve_df(curve_id, curve_metadata, symbol_metadata):
    curve_metadata = curve_metadata[curve_id]
    df_curve = pd.DataFrame()
    for symbol in curve_metadata[SYMBOLS]:
        # call hdq api
        method = 'TimeSeries'
        kwargs = {'method': method,
                  'symbol': symbol}
        df = getHdqPbAsDF(expression=hdq_expression.format(**kwargs))
        # rename columns and set index
        df = clean_hdq_timeseries(df, symbol)
        # add the contract start date from our metadata then move to the index
        df[CONTRACT_START] = symbol_metadata[symbol][CONTRACT_START]
        df.set_index(CONTRACT_START, drop=True, append=True, inplace=True)
        # add the curve_id as a column then move to the index
        df[CURVE_ID] = curve_id
        df.set_index(CURVE_ID, drop=True, append=True, inplace=True)
        # stack the df from each symbol
        df_curve = pd.concat([df_curve, df], axis='rows')
    # now we can remove the symbol
    df_curve.index = df_curve.index.droplevel(SYMBOL)
    # and reorder
    df_curve = df_curve.reorder_levels([CURVE_ID, CONTRACT_START, OBSERVATION_DATE], axis='index')
    return df_curve


def get_expiries_for_single_curve(curve_id, curve_metadata, symbol_metadata):
    curve_metadata = curve_metadata[curve_id]
    df_expiries = pd.DataFrame()
    for symbol in curve_metadata[SYMBOLS]:
        method = 'FutureExpiry'
        kwargs = {'method': method,
                  'symbol': symbol}
        df = getHdqPbAsDF(expression=hdq_expression.format(**kwargs))
        df = clean_hdq_expiry(df, symbol=symbol)
        # add the contract start date from our metadata then move it to the index
        df[CONTRACT_START] = symbol_metadata[symbol][CONTRACT_START]
        df.set_index(CONTRACT_START, drop=True, append=True, inplace=True)
        # add the curve_id as a column then move to the index
        df[CURVE_ID] = curve_id
        df.set_index(CURVE_ID, drop=True, append=True, inplace=True)
        # now we can remove the symbol
        df.index = df.index.droplevel(SYMBOL)
        # stack the df from each symbol
        df_expiries = pd.concat([df_expiries, df], axis='rows')
    # and reorder
    df_expiries = df_expiries.reorder_levels([CURVE_ID, CONTRACT_START], axis='index')
    return df_expiries


def enrich_ts(df, metadata_df):
    # horrific pandas join hack
    # https://pandas.pydata.org/pandas-docs/version/0.24.0/user_guide/merging.html
    df = pd.merge(df.reset_index(), metadata_df.reset_index(), left_on=(CURVE_ID, CONTRACT_START), right_on=(CURVE_ID, CONTRACT_START))
    df.set_index([CURVE_ID, OBSERVATION_DATE, CONTRACT_START], drop=True, inplace=True)
    return df


def calc_days_from_expiry(df):
    df[DAYS_FROM_EXPIRY] = df.index.get_level_values(OBSERVATION_DATE) - df[EXPIRY_DATE]


def reset_index_for_chart_data(df, index_columns):
    # we always need the curve_id
    index_columns.append(CURVE_ID)
    df.reset_index(inplace=True)
    df.set_index(index_columns, drop=True, inplace=True)
    return df[VALUE]


def pivot_for_chart_data(s, chart_index):
    df = s.to_frame()
    df.reset_index(inplace=True)
    df = df[[chart_index, LEGEND, VALUE]]
    df = df.pivot(index=chart_index, columns=LEGEND)
    # the value column name is added to the column index
    # remove it here
    df.columns = df.columns.droplevel(None)
    return df


def create_legend_index_for_chart_data(df, legends_mapper, names):
    mapper_index = pd.MultiIndex.from_tuples(zip(*legends_mapper))
    mapper_index.names = names
    return df.index.join(mapper_index)


def add_index_to_df(df, new_index):
    df.index = new_index
    return df


if __name__ == '__main__':
    curve_id = 1000000
    legends_mapper = [pd.DatetimeIndex(['2019-12-01', '2020-12-01', '2021-12-01']),
                      ['Dec2019', 'Dec2020', 'Dec2021']]
    legends_names = [CONTRACT_START, LEGEND]

    # call hdq api for all symbols that are members of this curve_id
    # returns a df of expiries
    df_expiries = get_expiries_for_single_curve(curve_id=curve_id, curve_metadata=curve_metadata, symbol_metadata=symbol_metadata)

    # call hdq api for all symbols that are members of this curve_id
    # returns the correct timeseries structure for a forward curve which can be stacked
    df_ts = build_single_curve_df(curve_id=curve_id, curve_metadata=curve_metadata, symbol_metadata=symbol_metadata)

    # enrich with expiry dates
    df_ts_enrich = enrich_ts(df_ts, df_expiries)
    # and then calc days from expiry
    calc_days_from_expiry(df_ts_enrich)
    
    # make days_from_expiry the new index
    df = reset_index_for_chart_data(df_ts_enrich, [CONTRACT_START, DAYS_FROM_EXPIRY])

    # add legend to dataframe
    index = create_legend_index_for_chart_data(df, legends_mapper, legends_names)
    df = add_index_to_df(df, index)

    # pivot the data ready to chart it
    df = pivot_for_chart_data(df, chart_index=DAYS_FROM_EXPIRY)

    df.plot(kind='line')
    plt.show()
    print('great')

