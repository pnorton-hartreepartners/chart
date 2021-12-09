import pandas as pd
from pprint import pprint as pp

trader_curve_symbols = ['BRT-F', 'EBOB-S', 'GO-F', 'NAP']

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


if __name__ == '__main__':
    months = pd.date_range(start='2021-01-01', periods=13, freq='MS')
    months = months.strftime('%Y%m')
    spreads = list(zip(months[:-1], months[1:]))

    expression = 'EBOB-S'
    type = 'Combo'

    expression_template_dict['expression'] = expression
    expression_template_dict['type'] = type

    # each spread will be a chartlet
    chartlet_list = []
    for spread in spreads:
        front, back = spread
        expression_dict = expression_template_dict.copy()
        expression_dict['contracts'] = [front, '', back]

        chartlet_dict = chartlet_template_dict.copy()
        chartlet_dict['name'] = expression + ' | ' + front + ' minus ' + back
        chartlet_dict['curves'] = expression_dict
        chartlet_list.append(chartlet_dict)

    chart_template_dict['chartlets'] = chartlet_list
    pp(chart_template_dict)
    pass
