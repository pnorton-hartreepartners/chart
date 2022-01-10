import pandas as pd
from mosaic.mosaic_api_templates import api_config_dict
from mosaic.mosaic_wapi import build_partial_url_kwargs, build_url, post_any_api, process_chart_data

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
    return [[front, back] for front, back in zip(months[:-1], months[1:])]


def _build_timespread_chartlet_name(product, contract_list):
    front, back = contract_list
    return product + ' | ' + front + ' minus ' + back


def _split_timespread_chartlet_name(ss):
    return ss


def _build_outright_chartlet_name(product, contract_list):
    [front] = contract_list
    return product + ' | ' + front


def _build_outrights_back(end, periods):
    months = pd.date_range(end=end, periods=periods, freq='MS')
    return [[month.strftime('%Y%m')] for month in months]


def build_chartlets(expression, type_, contracts, name_func):
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
        curves_dict['contracts'] = contract_list
        chartlet_dict['curves'] = [curves_dict]

        chartlet_list.append(chartlet_dict)
    return chartlet_list


def build_and_call_api(trader_curves,
                       datum, periods,
                       contracts_func,
                       name_func,
                       env):
    api_name = 'getTraderCurveTS'
    results = []
    for expression, type_ in trader_curves:
        # build url
        template_url = api_config_dict[api_name]['url_template']
        url_kwargs = build_partial_url_kwargs(api_name, env=env)
        url = build_url(template_url=template_url, kwargs=url_kwargs)

        # build payload
        contracts = contracts_func(datum, periods=periods)
        chartlets = build_chartlets(expression=expression, type_=type_,
                                    contracts=contracts,
                                    name_func=name_func)
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
    if df.empty:
        norm_df = pd.DataFrame()
    else:
        norm_df = pd.melt(df, value_vars=df.columns, ignore_index=False)
        # so unpivot the data
        # clean it
        norm_df[['symbol', 'contract']] = norm_df['variable'].str.split(pat='|').apply(
            lambda x: [e.strip() for e in x]).tolist()
        norm_df.drop(columns='variable', inplace=True)
        norm_df.set_index(keys=['symbol', 'contract'], drop=True, append=True, inplace=True)
        norm_df.dropna(axis='index', inplace=True)
    return norm_df


if __name__ == '__main__':
    pass

