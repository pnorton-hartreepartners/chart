import os
from itertools import combinations_with_replacement
import pandas as pd
from chart_data import build_and_call_api, build_clean_dataset, _build_timespreads, \
    _build_timespread_chartlet_name
from mosaic.constants import DEV, path, file_for_data, file_for_reduced_data, xlsx_for_results


trader_curves = [('BRT-F', 'Dynamic'),
                 ('EBOB-S', 'Combo'),
                 ('GO-F', 'Dynamic'),
                 ('NAP', 'Combo')]


def calc_correlation(df):
    group_list = ['symbol_x', 'contract_x', 'symbol_y', 'contract_y']
    value_list = ['value_x', 'value_y']
    # calculate correlation and count of observations
    corr_matrix_df = df.groupby(group_list)[value_list].corr(method='pearson')
    count_df = df.groupby(group_list).count()['value_x']

    # function returns a matrix for each pair; we only want an upper (or lower) triangle value
    mask = corr_matrix_df.index.get_level_values(None) == 'value_y'
    corr_df = corr_matrix_df[mask]['value_x']
    corr_df.index = corr_df.index.droplevel(level=None)

    # rename and join
    corr_df.name = 'corr'
    count_df.name = 'count'
    return pd.merge(corr_df, count_df, left_index=True, right_index=True)


def build_self_join_data(df):
    # drop these components of the multiindex
    # leaving only the observation date in the index
    df.reset_index(['symbol', 'contract'], drop=False, inplace=True)
    # join on itself
    return pd.merge(df, df, how='inner',
                        left_index=True, right_index=True,
                        suffixes=['_x', '_y'])


def build_reduced_cartesian_product(df):
    unique_index = df[['symbol_x', 'contract_x']].drop_duplicates(ignore_index=True).values
    pairs = list(combinations_with_replacement(unique_index, 2))
    masks = []
    for left, right in pairs:
        print(left, right)
        symbol_x, contract_x = left
        symbol_y, contract_y = right
        mask = (df['symbol_x'] == symbol_x) & \
               (df['contract_x'] == contract_x) & \
               (df['symbol_y'] == symbol_y) & \
               (df['contract_y'] == contract_y)
        masks.append(mask)
    super_mask = pd.concat(masks, how='outer', axis='columns').any(axis='columns')
    return df[super_mask]


if __name__ == '__main__':
    env = DEV

    build_and_save_data = True
    calc_and_save_corr = True

    if build_and_save_data:
        # build the data
        results = build_and_call_api(trader_curves, datum='2021-01-01', periods=13, env=env,
                                     contracts_func=_build_timespreads,
                                     name_func=_build_timespread_chartlet_name
                                     )
        data_df = build_clean_dataset(results)
        pathfile = os.path.join(path, file_for_data)
        data_df.to_pickle(pathfile)

    else:
        # load the data
        pathfile = os.path.join(path, file_for_data)
        data_df = pd.read_pickle(pathfile)

    # build the data ready for analysis
    cartesian_product_df = build_self_join_data(data_df)
    reduced_cartesian_product_df = build_reduced_cartesian_product(cartesian_product_df)

    pathfile = os.path.join(path, file_for_reduced_data)
    reduced_cartesian_product_df.to_pickle(pathfile)

    if calc_and_save_corr:
        correlation_df = calc_correlation(cartesian_product_df)
        correlation_df.to_clipboard()

        # save the results
        pathfile = os.path.join(path, xlsx_for_results)
        with pd.ExcelWriter(pathfile) as writer:
            correlation_df.to_excel(writer, merge_cells=False, sheet_name='corr')
