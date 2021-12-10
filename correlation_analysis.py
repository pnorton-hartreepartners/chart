import pandas as pd
import os
from pprint import pprint as pp
import itertools

from mosaic.constants import DEV, path
from mosaic.mosaic_api_templates import api_config_dict
from mosaic.mosaic_wapi import build_partial_url_kwargs, build_url, post_any_api, process_chart_data

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


def build_timespread_payload(expression, type, start='2021-01-01', periods=13):
    months = pd.date_range(start=start, periods=periods, freq='MS')
    months = months.strftime('%Y%m')
    contracts = list(zip(months[:-1], months[1:]))

    expression_template_dict['expression'] = expression
    expression_template_dict['type'] = type

    # each spread will be a chartlet
    chartlet_list = []
    for contract in contracts:
        front, back = contract
        expression_dict = expression_template_dict.copy()
        expression_dict['contracts'] = [front, back]

        chartlet_dict = chartlet_template_dict.copy()
        chartlet_dict['name'] = expression + ' | ' + front + ' minus ' + back
        chartlet_dict['curves'] = [expression_dict]
        chartlet_list.append(chartlet_dict)
    chart_template_dict['chartlets'] = chartlet_list
    pp(chart_template_dict)
    return chart_template_dict


def build_and_save_clean_data(trader_curves):
    api_name = 'getTraderCurveTS'
    for expression, type_ in trader_curves:
        payload = build_timespread_payload(expression=expression, type=type_, start='2021-01-01', periods=13)

        template_url = api_config_dict[api_name]['url_template']
        url_kwargs = build_partial_url_kwargs(api_name, env=env)
        url = build_url(template_url=template_url, kwargs=url_kwargs)

        # call the api
        result = post_any_api(url, payload=payload)

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

        # save it
        file_for_df = expression + '.pkl'
        pathfile = os.path.join(path, file_for_df)
        norm_df.to_pickle(pathfile)


def build_correlation_data():
    all_keys_dict = {}
    all_df = pd.DataFrame()
    for expression, type_ in trader_curves:
        all_keys_dict[expression] = {}
        file_for_df = expression + '.pkl'
        pathfile = os.path.join(path, file_for_df)
        df = pd.read_pickle(pathfile)

        # get keys
        multiindex = list(df.index)
        multiindex = [(i[1], i[2]) for i in multiindex]  # sorry
        keys = list(set(multiindex))
        all_keys_dict[expression] = keys

        # one big df with all the lovely data
        all_df = pd.concat([all_df, df], axis='index')

    # from dict to list of lists
    all_keys = [all_keys_dict[expression] for expression in all_keys_dict.keys()]
    # create pairs eg selecting two from four products results in six pairs
    corr_list = list(itertools.combinations(all_keys, 2))
    charts_dict = {}
    for product_x, product_y in corr_list:
        # then for each of the pairs we need a cartesian product of spreads
        # eg if each has ten months of spreads then we get 100 correlations to check
        charts = list(itertools.product(product_x, product_y))
        for chart in charts:
            x, y = chart
            x_symbol, x_contract = x
            x_data = all_df.loc[slice(None), x_symbol, x_contract]

            y_symbol, y_contract = y
            y_data = all_df.loc[slice(None), y_symbol, y_contract]

            chart_df = pd.merge(x_data, y_data, how='inner', left_index=True, right_index=True, suffixes=['_x', '_y'])

            key = ((x_symbol, x_contract), (y_symbol, y_contract))
            charts_dict[key] = {}
            charts_dict[key]['df'] = chart_df
            charts_dict[key]['shape'] = chart_df.shape

    return charts_dict


if __name__ == '__main__':
    env = DEV
    build_and_save_clean_data(trader_curves)
    charts_dict = build_correlation_data()

    pass

