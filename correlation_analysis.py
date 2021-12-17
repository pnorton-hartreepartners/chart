import pandas as pd
import os
from mosaic.constants import DEV, path
from mosaic.mosaic_api_templates import api_config_dict
from mosaic.mosaic_wapi import build_partial_url_kwargs, build_url, post_any_api, process_chart_data
import plotly.express as px


file_for_data = 'correlation_data.pkl'
xlsx_for_results = 'correlation_results.xlsx'

trader_curves = [('BRT-F', 'Dynamic'),
                 ('EBOB-S', 'Combo'),
                 ('GO-F', 'Dynamic'),
                 ('NAP', 'Combo')]

chart_template_dict = {
                          'start_date': '2015-01-01',
                          'end_date': '2024-01-01',
                          'seasonality': 0,
                          'chartlets': None  # this is a list of chartlets
                      }

chartlet_template_dict = {
        'name': None,
        'curves': None,  # this is a list of expressions
        'currency': 'USD',
        'uom': '*',
        'axis': 0
    }

expression_template_dict = {
    'expression': None,
    'factor': 1,
    'type': None,
    'contracts': None,  # this is a list of 3x periods
}


def _build_timespreads(start, periods):
    months = pd.date_range(start=start, periods=periods, freq='MS')
    months = months.strftime('%Y%m')
    return list(zip(months[:-1], months[1:]))


def _build_timespread_chartlet_name(product, contract_list):
    front, back = contract_list
    return product + ' | ' + front + ' minus ' + back


def _split_timespread_chartlet_name(ss):
    return ss


def _build_chartlets(expression, type_, contracts, name_func):
    # each contract will be a chartlet ie a line on the chart
    chartlet_list = []
    for contract_list in contracts:
        # chartlet_dict contains curves_dict
        chartlet_dict = chartlet_template_dict.copy()
        curves_dict = expression_template_dict.copy()

        # update values
        chartlet_dict['name'] = name_func(expression, contract_list)
        curves_dict['expression'] = expression
        curves_dict['type'] = type_
        curves_dict['contracts'] = list(contract_list)
        chartlet_dict['curves'] = [curves_dict]

        chartlet_list.append(chartlet_dict)
    return chartlet_list


def build_and_call_api(trader_curves, start, periods):
    api_name = 'getTraderCurveTS'
    results = []
    for expression, type_ in trader_curves:
        # build url
        template_url = api_config_dict[api_name]['url_template']
        url_kwargs = build_partial_url_kwargs(api_name, env=env)
        url = build_url(template_url=template_url, kwargs=url_kwargs)

        # build payload
        contracts = _build_timespreads(start=start, periods=periods)
        chartlets = _build_chartlets(expression=expression, type_=type_,
                                     contracts=contracts,
                                     name_func=_build_timespread_chartlet_name)
        chart_template_dict['chartlets'] = chartlets

        # call the api
        result = post_any_api(url, payload=chart_template_dict)
        results.append(result)
    return results


def build_clean_dataset(results):
    all_df = pd.DataFrame()
    for result in results:
        # process the result
        df = _clean_api_result(result)
        # one big df with all the lovely data
        all_df = pd.concat([all_df, df], axis='index')
    return all_df


def _clean_api_result(result):
    # process result and return a df if possible; but it will be pivotted
    df = process_chart_data(result, already_pivotted=True)
    # so unpivot the data
    norm_df = pd.melt(df, value_vars=df.columns, ignore_index=False)
    # clean it
    norm_df[['symbol', 'contract']] = norm_df['variable'].str.split(pat='|').apply(
        lambda x: [e.strip() for e in x]).tolist()
    norm_df.drop(columns='variable', inplace=True)
    norm_df.set_index(keys=['symbol', 'contract'], drop=True, append=True, inplace=True)
    norm_df.dropna(axis='index', inplace=True)
    return norm_df


def calc_correlation(df):
    group_list = ['symbol_x', 'contract_x', 'symbol_y', 'contract_y']
    value_list = ['value_x', 'value_y']
    # calculate correlation and count of observations
    corr_matrix_df = df.groupby(group_list)[value_list].corr(method='pearson')
    count_df = df.groupby(group_list).count()['value_x']

    # function returns a matrix; we only want an upper (or lower) triangle value
    mask = corr_matrix_df.index.get_level_values(None) == 'value_y'
    corr_df = corr_matrix_df[mask]['value_x']
    corr_df.index = corr_df.index.droplevel(level=None)

    # rename and join
    corr_df.name = 'corr'
    count_df.name = 'count'
    return pd.merge(corr_df, count_df, left_index=True, right_index=True)


def build_self_join_data(df):
    df.reset_index(['symbol', 'contract'], drop=False, inplace=True)
    chart_df = pd.merge(df, df, how='inner',
                        left_index=True, right_index=True,
                        suffixes=['_x', '_y'])
    return chart_df


def create_scatter_matrix(df):
    contract = '202110 minus 202111'
    mask_x = df['contract_x'] == contract
    mask_y = df['contract_y'] == contract
    selection_df = df[mask_x & mask_y]
    fig = px.scatter(data_frame=selection_df,
                     x='value_x',
                     y='value_y',
                     facet_col='symbol_x', facet_row='symbol_y',
                     trendline='ols')
    fig.show()


if __name__ == '__main__':
    env = DEV
    pathfile = os.path.join(path, file_for_data)

    build_and_save_data = False
    calc_and_save_corr = True
    create_scatter_charts = True

    if build_and_save_data:
        # build the data
        results = build_and_call_api(trader_curves, start='2021-01-01', periods=13)
        data_df = build_clean_dataset(results)
        data_df.to_pickle(pathfile)

    else:
        # load the data
        data_df = pd.read_pickle(pathfile)

    # build the data ready for analysis
    cartesian_product_df = build_self_join_data(data_df)

    if calc_and_save_corr:
        correlation_df = calc_correlation(cartesian_product_df)
        correlation_df.to_clipboard()

        # save the results
        pathfile = os.path.join(path, xlsx_for_results)
        with pd.ExcelWriter(pathfile) as writer:
            correlation_df.to_excel(writer, merge_cells=False, sheet_name='corr')

    if create_scatter_charts:
        create_scatter_matrix(cartesian_product_df)
