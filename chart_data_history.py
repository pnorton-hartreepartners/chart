import pandas as pd

from chart_data import build_and_call_api, build_clean_dataset, _build_outrights_back, _build_outright_chartlet_name, \
    _build_outright_decembers_back, _build_single_outright
from mosaic.constants import DEV, PROD

trader_curves = [('BRT-F', 'Dynamic'),
                 ('WTI-F', 'Dynamic'),
                 ('GO-F', 'Dynamic'),
                 ('HO-F', 'Dynamic'),
                 ('RB-F', 'Dynamic'),
                # ('EUA', 'Dynamic')
                 ]


if __name__ == '__main__':
    env = PROD

    months = pd.date_range(start='2010-12-01', periods=5 * 12, freq='MS')
    months = [month for month in months if month.month == 12]

    # build the data
    all_data_df = pd.DataFrame()
    for month in months:
        results = build_and_call_api(trader_curves, datum=month, periods=1, env=env,
                                     contracts_func=_build_single_outright,
                                     name_func=_build_outright_chartlet_name
                                     )
        data_df = build_clean_dataset(results)
        all_data_df = pd.concat([all_data_df, data_df], axis='index')
        pass

    xx = all_data_df.reset_index()
    xx.drop(axis='columns', labels=['value'], inplace=True)
    result = xx.groupby(by=['symbol', 'contract']).min()
    result.sort_values(by='time', inplace=True, ascending=False)
    result.to_clipboard()

    pass
