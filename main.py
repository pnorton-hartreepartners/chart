import pandas as pd
import matplotlib.pyplot as plt
from constants import CURVE_ID, OBSERVATION_DATE, CONTRACT_START, EXPIRY_DATE, DAYS_FROM_EXPIRY, VALUE, LEGEND, PROCESSOR
from processors import curve_metadata

config_rbob_ebob_builtup_arb = [
    ["Product", "Def code", "Factor", "Period", "Qty"],
    ["RBOB Cal Swap", "NYMRBOBCalSwap", 1.0000, 1.00, 100.00],
    ["EBOB (Argus Gas FOB ARA)", "ArgGASFBARA", 0.0029, 1.00, -65.00],
    ["Naphtha CIF NWE", "NAPphCIFNWE", 0.0027, 1.00, -35.00],
    ["TC2 $/ton", "TC2_USDMT_", 0.0032, 1.00, -100.00],
    ["RINRVOCost", "RINRVOCost", 1.0000, 1.00, -100.00]]


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
    Processor = curve_metadata[curve_id][PROCESSOR]

    legends_mapper = [pd.DatetimeIndex(['2019-12-01', '2020-12-01', '2021-12-01']),
                      ['Dec2019', 'Dec2020', 'Dec2021']]
    legends_names = [CONTRACT_START, LEGEND]

    p = Processor()
    p.build_single_curve_df(curve_id)
    p.get_expiries_for_selected_curve()

    # enrich curve with expiry dates
    df_ts_enrich = enrich_ts(p.df_curve, p.df_expiries)
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

