import urllib.request
from urllib.parse import quote_plus as qp
import pandas as pd
from datetime import datetime, date, timedelta
from protobuf_hdq import hdq_pb2

#HDQ_IP = '10.14.5.47'  # WARNING : http://nyweb01 when running locally
HDQ_IP = r'nyweb01'

# required for logging
username           = qp('mosaic')
machinename        = qp('aws')
applicationname    = qp('hdq python')
applicationversion = qp('1')


def getHdqHtmlAsDF(expression, replace_ws=False): # WARNING : try not to use it if the protobuf version works
    """
    Some possible expressions :
        expression = "TimeSeries('IPEBRT19Z')"
        expression = "FutureExpiry('IPEBRT19Z')"
        expression = "CommodityFutureExpirySchedule('BRT')"
        expression = "CommodityInstrumentMapping()"
        expression = "RunSql('PFS','Select%20*%20From%20Commodity_Specifications')"
        expression = "PfsForwardCurveGroup('Hetco','WTI,BRT',date(2018,6,20))"
        expression = "NearbyTimeSeries('IPEBRT',1,Utctoday()-250,UtcToday())"
        expression = "FabricImpliedVols('2019-7-8','WTI')" # WTI, BRT, GOLD, COPPER
 
    Use replace_ws=True to handle :
        expression = "RunSql('PFS','Select * From Commodity_Specifications')"
    """
    #
    if replace_ws:
        expression = ' '.join(expression.split())
        expression = expression.replace(' ', '%20')
    #
    url = 'http://'+HDQ_IP+'/HdqQueryTester/?q='+expression+\
          '&username='+username+'&machinename='+machinename+\
          '&applicationname='+applicationname+'&applicationversion='+applicationversion
    #
    with urllib.request.urlopen(url) as hdq_page:
        hdq_html = hdq_page.read()
        hdq_web_tables = pd.read_html(hdq_html)
        hdq_df = pd.DataFrame(hdq_web_tables[1])
        hdq_df.columns = hdq_df.iloc[0] # putting the column name in columns
        hdq_df.drop(hdq_df.index[0], inplace=True) # removing row containing row names
    #
    return hdq_df

     
def getHdqPbAsDF(expression, replace_ws=False):
    """
    Some possible expressions :
        expression = "TimeSeries('IPEBRT19Z')"
        expression = "FutureExpiry('IPEBRT19Z')"
        expression = "CommodityFutureExpirySchedule('BRT')"
        expression = "CommodityInstrumentMapping()"
        expression = "RunSql('PFS','Select%20*%20From%20Commodity_Specifications')"
         
    Use replace_ws=True to handle :
        expression = "RunSql('PFS','Select * From Commodity_Specifications')"
    """
    #
    if replace_ws:
        expression = ' '.join(expression.split())
        expression = expression.replace(' ', '%20')
    #
    url = 'http://'+HDQ_IP+'/HdqWS/pb.aspx?expr='+expression+\
          '&username='+username+'&machinename='+machinename+\
          '&applicationname='+applicationname+'&applicationversion='+applicationversion
    #
    epoch = datetime(1970,1,1) # to parse dates
    #
    with urllib.request.urlopen(url) as hdq_page:   
        hdq_html = hdq_page.read()
        obj = hdq_pb2.ProtoBufData()
        obj.ParseFromString(hdq_html)
        headers = obj.Headers
        data = obj.Data
        ncols = len(headers)
        #
        Rows = [data[i:i+ncols] for i in range(0, len(data), ncols)]
        #
        Ls = []
        for row in Rows:
            dic = {}
            for i, datum in enumerate(row):
                if datum.HasField('Number'):
                    dic[ headers[i] ] = datum.Number
                elif datum.HasField('String'):
                    dic[ headers[i] ] = datum.String
                elif datum.HasField('Date'):
                    dic[ headers[i] ] = epoch + timedelta(days = datum.Date.value)
            #
            Ls.append(dic)
        #
    #
    return pd.DataFrame(Ls)


def TimeSeries(symbol): # eg symbol='IPEBRT19Z'
    df = getHdqPbAsDF("TimeSeries('{smbl}')".format(smbl=symbol), replace_ws=False)
    ts = df.set_index('Date', inplace=False)
    ts.sort_index(axis=0, ascending=True, inplace=True)
    ts.rename(index=str, columns={ts.columns[0]: symbol}, inplace=True)
    return ts


def NearbyTimeSeries(symbol): # eg symbol='IPEBRT' # WARNING : should have a start and end input too
    df = getHdqPbAsDF("NearbyTimeSeries('{smbl}',1,Utctoday()-250,UtcToday())".format(smbl=symbol), replace_ws=False)
    ts = df.set_index('Date', inplace=False)
    ts.sort_index(axis=0, ascending=True, inplace=True)
    ts.rename(index=str, columns={ts.columns[0]: symbol}, inplace=True)
    return ts


def FutureExpiry(symbol): # eg symbol='IPEBRT19Z'
    df = getHdqPbAsDF("FutureExpiry('{smbl}')".format(smbl=symbol), replace_ws=False)
    assert df.shape == (1,1), 'not handled'
    dt = df.values[0][0]
    return pd.Timestamp(dt).to_pydatetime().date()


def CommodityFutureExpirySchedule(symbol): # eg symbol='BRT'
    return getHdqPbAsDF("CommodityFutureExpirySchedule('{smbl}')".format(smbl=symbol), replace_ws=False)


def ForwardCurve(symbol, stamp=None): # eg symbol='NYMWTI' and stamp=date(2019,6,24) or stamp='2019-06-24'
    if stamp is None:
        stamp = 'UtcLastWeekday()'
    elif type(stamp) is date:
        stamp = 'date({},{},{})'.format(stamp.year, stamp.month, stamp.day)
    else:
        raise ValueError('not handled')
    #
    return getHdqPbAsDF("ForwardCurveGroup('{smbl}',{stmp})".format(smbl=symbol, stmp=stamp), replace_ws=False)


def ForwardCurveGroup(symbols, stamp=None): # eg symbols=['NYMWTI','IPEBRT','NYMHO']
    if stamp is None:
        stamp = 'UtcLastWeekday()'
    elif type(stamp) is date:
        stamp = 'date({},{},{})'.format(stamp.year, stamp.month, stamp.day)
    else:
        raise ValueError('not handled')
    #
    assert not(True in [(',' in smbl) for smbl in symbols]), 'symbols should not contain any comma'
    #
    return getHdqPbAsDF("ForwardCurveGroup('{smbls}',{stmp})".format(smbls=','.join(symbols), stmp=stamp), replace_ws=False)


def RunSql(query, replace_ws=True): # eg query='Select * From Commodity_Specifications'
    return getHdqPbAsDF("RunSql('PFS','{qry}')".format(qry=query), replace_ws)


