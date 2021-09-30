import datetime as dt
from constants import CONTRACT_START, LONG_SCREEN_NAME, UOM, CURRENCY

symbol_metadata = {
    'IPEBRT19Z': {CONTRACT_START: dt.date(2019, 12, 1)},
    'IPEBRT20Z': {CONTRACT_START: dt.date(2020, 12, 1)},
    'IPEBRT21Z': {CONTRACT_START: dt.date(2021, 12, 1)}
}

curve_metadata = {
    1000000: {
        'symbols': ['IPEBRT19Z', 'IPEBRT20Z', 'IPEBRT21Z'],
        'names': {LONG_SCREEN_NAME: 'IPE Brent Futures'},
        UOM: 'bbl',
        CURRENCY: 'USD'
    },
    1000001: {
        'symbols': ['CMEWTI19Z', 'CMEWTI20Z', 'CMEWTI21Z'],
        'names': {LONG_SCREEN_NAME: 'CME WTI Futures'},
        UOM: 'bbl',
        CURRENCY: 'USD'
    },
}