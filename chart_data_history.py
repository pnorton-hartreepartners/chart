from chart_data import build_and_call_api, build_clean_dataset, _build_outrights_back, _build_outright_chartlet_name
from mosaic.constants import DEV, PROD

trader_curves = [('BRT-F', 'Dynamic'),
                 ('WTI-F', 'Dynamic'),
                 ('GO-F', 'Dynamic'),
                 ('HO-F', 'Dynamic'),
                 ('RB-F', 'Dynamic'),
                 ('EUA', 'Dynamic')
                 ]


if __name__ == '__main__':
    env = PROD

    # build the data
    results = build_and_call_api(trader_curves, datum='2020-12-01', periods=36, env=env,
                                 contracts_func=_build_outrights_back,
                                 name_func=_build_outright_chartlet_name
                                 )
    data_df = build_clean_dataset(results)

    xx = data_df.reset_index()
    xx.drop(axis='columns', labels=['value'], inplace=True)
    result = xx.groupby(by=['symbol', 'contract']).min()
    result.sort_values(by='time', inplace=True, ascending=False)
    result.to_clipboard()

    pass
