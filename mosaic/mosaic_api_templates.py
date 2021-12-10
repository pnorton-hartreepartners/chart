SETTLES = 'settles'

api_config_dict = {
    # trader curve time-series backed by tempest for history
    'getTraderCurveTS': {'host': SETTLES,
                         'method': 'post',
                         'url_template': r'{host}/api/v1/getTraderCurveTS'}
}
