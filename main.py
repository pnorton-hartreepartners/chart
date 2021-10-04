import pandas as pd
import matplotlib.pyplot as plt
from constants import CURVE_ID, OBSERVATION_DATE, CONTRACT_START, EXPIRY_DATE, DAYS_FROM_EXPIRY, VALUE, LEGEND, \
    LONG_SCREEN_NAME, PROCESSOR
from processors import curve_metadata

config_rbob_ebob_builtup_arb = [
    ["Product", "Def code", "Factor", "Period", "Qty"],
    ["RBOB Cal Swap", "NYMRBOBCalSwap", 1.0000, 1.00, 100.00],
    ["EBOB (Argus Gas FOB ARA)", "ArgGASFBARA", 0.0029, 1.00, -65.00],
    ["Naphtha CIF NWE", "NAPphCIFNWE", 0.0027, 1.00, -35.00],
    ["TC2 $/ton", "TC2_USDMT_", 0.0032, 1.00, -100.00],
    ["RINRVOCost", "RINRVOCost", 1.0000, 1.00, -100.00]]


def enrich_ts(df, metadata_df, ts_join, metadata_join, new_index_columns, drop_remaining=True):
    # horrific pandas join hack
    # https://pandas.pydata.org/pandas-docs/version/0.24.0/user_guide/merging.html
    df = pd.merge(df.reset_index(), metadata_df.reset_index(), left_on=ts_join, right_on=metadata_join)
    df.set_index(new_index_columns, drop=True, inplace=True)

    if drop_remaining:
        return df[VALUE]
    else:
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


def create_mapper_index(legends_mapper, names):
    mapper_index = pd.MultiIndex.from_tuples(zip(*legends_mapper))
    mapper_index.names = names
    return mapper_index


if __name__ == '__main__':
    curve_id = 1000000
    Processor = curve_metadata[curve_id][PROCESSOR]

    p = Processor()
    p.build_single_curve_df(curve_id)
    p.get_expiries_for_selected_curve()

    # ==================================================================================================================
    # some trivial legend mappings

    legends_mapper = [pd.DatetimeIndex(['2019-12-01', '2020-12-01', '2021-12-01']),
                      ['Z19', 'Z20', 'Z21']]
    legends_names = [CONTRACT_START, LEGEND]
    mapper_index = create_mapper_index(legends_mapper, legends_names)

    # ==================================================================================================================
    # add metadata

    # add a curve name
    # build the metadata
    data = [curve_metadata[curve_id]['names'][LONG_SCREEN_NAME]]
    index = pd.Index([curve_id], name=CURVE_ID)
    df_name = pd.DataFrame(data=data, index=index, columns=[LONG_SCREEN_NAME])
    # define the join info
    left_on = [CURVE_ID]
    right_on = [CURVE_ID]
    new_index_columns = [LONG_SCREEN_NAME, OBSERVATION_DATE, CONTRACT_START]
    # call the helper function
    df_ts_label = enrich_ts(p.df_curve, df_name, ts_join=left_on, metadata_join=right_on,
                            new_index_columns=new_index_columns, drop_remaining=True)
    df_ts_label.dropna(axis='index', inplace=True)
    df_ts_label.to_clipboard()

    # add expiry data
    left_on = [CURVE_ID, CONTRACT_START]
    right_on = [CURVE_ID, CONTRACT_START]
    new_index_columns = [CURVE_ID, OBSERVATION_DATE, CONTRACT_START]
    df_ts_expiry = enrich_ts(p.df_curve, p.df_expiries, ts_join=left_on, metadata_join=right_on,
                             new_index_columns=new_index_columns, drop_remaining=False)
    df_ts_expiry.dropna(axis='index', inplace=True)
    df_ts_expiry.to_clipboard()

    # add contract period names as a legend
    df_legend = pd.DataFrame(index=mapper_index)
    # define the join info
    left_on = [CONTRACT_START]
    right_on = [CONTRACT_START]
    new_index_columns = [LEGEND, OBSERVATION_DATE, CONTRACT_START]
    # call the helper function
    df_ts_legend = enrich_ts(p.df_curve, df_legend, ts_join=left_on, metadata_join=right_on,
                            new_index_columns=new_index_columns, drop_remaining=True)
    df_ts_legend.dropna(axis='index', inplace=True)
    df_ts_legend.to_clipboard()

    # ==================================================================================================================
    # using the dataset enriched with expiry data we can calculate curves showing seasonality

    # calc days from expiry
    calc_days_from_expiry(df_ts_expiry)
    
    # make days_from_expiry the new index
    df = reset_index_for_chart_data(df_ts_expiry, [CONTRACT_START, DAYS_FROM_EXPIRY])

    # add legend to dataframe
    df.index.join(mapper_index)
    df.index = df.index.join(mapper_index)

    # pivot the data ready to chart it
    df = pivot_for_chart_data(df, chart_index=DAYS_FROM_EXPIRY)

    df.plot(kind='line')
    plt.show()
    print('great')

