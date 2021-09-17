import datetime as dt
from constants import CONTRACT_START, LONG_SCREEN_NAME, UOM, CURRENCY, EXPIRY_DATE

metadata = {'IPEBRT19Z': {
    CONTRACT_START: dt.date(2019, 12, 1),
    LONG_SCREEN_NAME: 'IPE Brent Futures Dec 19',
    CURRENCY: 'USD',
    UOM: 'barrel',
    EXPIRY_DATE: dt.date(2019, 10, 31)  # IPEBRT19Z  2019-10-31
},
'IPEBRT20Z': {
    CONTRACT_START: dt.date(2020, 12, 1),
    LONG_SCREEN_NAME: 'IPE Brent Futures Dec 20',
    CURRENCY: 'USD',
    UOM: 'barrel',
    EXPIRY_DATE: dt.date(2020, 10, 30)  # IPEBRT20Z  2020-10-30
},
'IPEBRT21Z': {
    CONTRACT_START: dt.date(2021, 12, 1),
    LONG_SCREEN_NAME: 'IPE Brent Futures Dec 21',
    CURRENCY: 'USD',
    UOM: 'barrel',
    EXPIRY_DATE: dt.date(2021, 10, 29)  # IPEBRT21Z  2021-10-29
}
}

