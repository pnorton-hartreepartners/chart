from protobuf_hdq import hdq_pb2
import datetime
import os
import urllib.request
import pandas

server = 'nyweb01' # for test: NYWEBTEST01

# required for logging
username           = urllib.parse.quote_plus(os.getenv('USERNAME'))
machinename        = urllib.parse.quote_plus(os.getenv('COMPUTERNAME'))
applicationname    = urllib.parse.quote_plus('testcode')
applicationversion = urllib.parse.quote_plus('1')


def runhdq(hdqExpression):
    url = "http://"+server+"/HdqWS/pb.aspx?expr="+urllib.parse.quote_plus(hdqExpression)+\
           "&username="+username+"&machinename="+machinename+\
           "&applicationname="+applicationname+"&applicationversion="+applicationversion
    with urllib.request.urlopen(url) as req:
        response = req.read()
        obj = hdq_pb2.ProtoBufData()
        obj.ParseFromString (response)
        headers = obj.Headers

        df = pandas.DataFrame()
        for header in headers:
            df[header] = {}

        epoch = datetime.datetime(1970,1,1)
        i = 0
        headerCount = len(df.columns)
        dic = {}
        for datum in obj.Data:
            if datum.HasField('Number'):
                dic[ headers[i] ] = datum.Number
            if datum.HasField('String'):
                dic[ headers[i] ] = datum.String
            if datum.HasField('Date'):
                dic[ headers[i] ] = epoch + datetime.timedelta( days = datum.Date.value )
            if i == headerCount-1:
                df = df.append(dic , ignore_index=True) 
                dic = {}
                i = -1
            i = i + 1

        return df
