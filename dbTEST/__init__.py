# import logging
# import datetime
# from datetime import datetime, timedelta
# import http.client
# import json
# import time
# import pandas as pd
# import numpy as np
# import ssl
# import certifi
# import azure.functions as func
# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# from io import StringIO


# def main(name: str) -> str:
    
#     username = 'suncor2'
#     password = 'anisoTropical308'
#     server = 'api.nrgstream.com'
#     token_path = '/api/security/token'
#     release_path = '/api/ReleaseToken'
#     token_payload = f'grant_type=password&username={username}&password={password}'
#     token_expiry = datetime.now() - timedelta(seconds=60)
#     access_token = ""

#     def get_token():
#         global access_token, token_expiry
#         if not is_token_valid():
#             headers = {"Content-type": "application/x-www-form-urlencoded"}
#             context = ssl.create_default_context(cafile=certifi.where())
#             conn = http.client.HTTPSConnection(server, context=context)
#             conn.request('POST', token_path, token_payload, headers)
#             res = conn.getresponse()
#             res_code = res.code

#             if res_code == 200:
#                 res_data = res.read()
#                 json_data = json.loads(res_data.decode('utf-8'))
#                 access_token = json_data['access_token']
#                 token_expiry = datetime.now() + timedelta(seconds=json_data['expires_in'])
#             else:
#                 res_data = res.read()
#                 print(res_data.decode('utf-8'))
#             conn.close()
#         return access_token

#     def release_token():
#         global token_expiry
#         if is_token_valid():
#             headers = {'Authorization': f'Bearer {access_token}'}
#             context = ssl.create_default_context(cafile=certifi.where())
#             conn = http.client.HTTPSConnection(server, context=context)
#             conn.request('DELETE', release_path, None, headers)
#             res = conn.getresponse()
#             res_code = res.code
#             if res_code == 200:
#                 token_expiry = datetime.now() - timedelta(seconds=60)

#     def is_token_valid():
#         return datetime.now() < token_expiry


#     def get_stream_data_by_stream_id(stream_ids, from_date, to_date, data_format='csv', data_option=''):
#         global access_token
#         stream_data = []
#         data_formats = {'csv': 'text/csv', 'json': 'Application/json'}

#         for stream_id in stream_ids:
#             access_token = get_token()
#             if is_token_valid():
#                 path = f'/api/StreamData/{stream_id}'
#                 params = []
#                 if from_date != '' and to_date != '':
#                     params.append(f'fromDate={from_date.replace(" ", "%20")}')
#                     params.append(f'toDate={to_date.replace(" ", "%20")}')
#                 if data_option != '':
#                     params.append(f'dataOption={data_option}')

#                 if params:
#                     path += '?' + '&'.join(params)

#                 headers = {'Accept': data_formats[data_format], 'Authorization': f'Bearer {access_token}'}

#                 context = ssl.create_default_context(cafile=certifi.where())
#                 conn = http.client.HTTPSConnection(server, context=context)
#                 conn.request('GET', path, None, headers)
#                 res = conn.getresponse()
#                 res_code = res.code

#                 if res_code == 200:
#                     if data_format == 'csv':
#                         stream_data.append(res.read().decode('utf-8').replace('\r\n', '\n'))
#                     elif data_format == 'json':
#                         stream_data.append(json.loads(res.read().decode('utf-8')))
#                     conn.close()
#         return stream_data 
        
    

#     fromDateStr = (datetime.now() - timedelta(hours=1)).strftime("%m/%d/%Y %H:%M")
#     toDateStr = fromDateStr

#     dataFormat = 'csv'
#     dataOption = ''

#     getByStream = True
#     stream_data_list = []

#     if getByStream:
#         column_names= ['CoalDCR','HydroDCR','GasDCR']
#         streamIds = [80,79,78]

#         df_all = pd.DataFrame()
#     for stream_id, column_name in zip(streamIds, column_names):
#         stream_data = get_stream_data_by_stream_id([stream_id], fromDateStr, toDateStr, dataFormat, dataOption)
#         if dataFormat == 'csv':
#             df = csv_stream_to_pandas(stream_data)
#             df.rename(columns={'Data Value': column_name}, inplace=True)
#             df.set_index('Date/Time', inplace=True)
#             df.index = pd.to_datetime(df.index)
#             df_all = pd.concat([df_all, df], axis=1)

#     df_all.columns = column_names
#     current = datetime.now()
#     current_td = timedelta(
#         hours = current.hour, 
#         minutes = current.minute, 
#         seconds = current.second, 
#         microseconds = current.microsecond)
#     to_hour = timedelta(hours = round(current_td.total_seconds() // 3600))
#     tohour = datetime.combine(current, dt.time(1)) + to_hour
#     print(tohour)
#     df_all = df_all.round().astype(int)

#     df_all = df_all.resample('H', label='right', closed='right').mean()
#     df_all = df_all[df_all.index < tohour]

#     upload_to_blob_storage(df_all, fromDateStr)
#     return "HistoricalDB has completed."




import logging
import datetime
from datetime import datetime, timedelta
import http.client
import json
import time
import pandas as pd
import numpy as np
import ssl
import certifi
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO

def csv_stream_to_pandas(csv_stream):
    return pd.read_csv(StringIO(csv_stream), skiprows=[0], header=None, names=['Date/Time', 'Data Value'])

def main(name: str) -> str:
    username = 'suncor2'
    password = 'anisoTropical308'
    server = 'api.nrgstream.com'
    token_path = '/api/security/token'
    release_path = '/api/ReleaseToken'
    token_payload = f'grant_type=password&username={username}&password={password}'
    token_expiry = datetime.now() - timedelta(seconds=60)
    access_token = ""

    def get_token():
        global access_token, token_expiry
        if not is_token_valid():
            headers = {"Content-type": "application/x-www-form-urlencoded"}
            context = ssl.create_default_context(cafile=certifi.where())
            conn = http.client.HTTPSConnection(server, context=context)
            conn.request('POST', token_path, token_payload, headers)
            res = conn.getresponse()
            res_code = res.code

            if res_code == 200:
                res_data = res.read()
                json_data = json.loads(res_data.decode('utf-8'))
                access_token = json_data['access_token']
                token_expiry = datetime.now() + timedelta(seconds=json_data['expires_in'])
            else:
                res_data = res.read()
                print(res_data.decode('utf-8'))
            conn.close()
        return access_token

    def release_token():
        global token_expiry
        if is_token_valid():
            headers = {'Authorization': f'Bearer {access_token}'}
            context = ssl.create_default_context(cafile=certifi.where())
            conn = http.client.HTTPSConnection(server, context=context)
            conn.request('DELETE', release_path, None, headers)
            res = conn.getresponse()
            res_code = res.code
            if res_code == 200:
                token_expiry = datetime.now() - timedelta(seconds=60)

    def is_token_valid():
        return datetime.now() < token_expiry


    def get_stream_data_by_stream_id(stream_ids, from_date, to_date, data_format='csv', data_option=''):
        global access_token
        stream_data = []
        data_formats = {'csv': 'text/csv', 'json': 'Application/json'}

        for stream_id in stream_ids:
            access_token = get_token()
            if is_token_valid():
                path = f'/api/StreamData/{stream_id}'
                params = []
                if from_date != '' and to_date != '':
                    params.append(f'fromDate={from_date.replace(" ", "%20")}')
                    params.append(f'toDate={to_date.replace(" ", "%20")}')
                if data_option != '':
                    params.append(f'dataOption={data_option}')

                if params:
                    path += '?' + '&'.join(params)

                headers = {'Accept': data_formats[data_format], 'Authorization': f'Bearer {access_token}'}

                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(server, context=context)
                conn.request('GET', path, None, headers)
                res = conn.getresponse()
                res_code = res.code

                if res_code == 200:
                    if data_format == 'csv':
                        stream_data.append(res.read().decode('utf-8').replace('\r\n', '\n'))
                    elif data_format == 'json':
                        stream_data.append(json.loads(res.read().decode('utf-8')))
                    conn.close()
        return stream_data 
        
    

    fromDateStr = (datetime.now() - timedelta(hours=1)).strftime("%m/%d/%Y %H:%M")
    toDateStr = fromDateStr

    dataFormat = 'csv'
    dataOption = ''

    getByStream = True
    stream_data_list = []
    
    if getByStream:
        column_names= ['CoalDCR','HydroDCR','GasDCR']
        streamIds = [80,79,78]

        df_all = pd.DataFrame()
    for stream_id, column_name in zip(streamIds, column_names):
        stream_data = get_stream_data_by_stream_id([stream_id], fromDateStr, toDateStr, dataFormat, dataOption)
        if dataFormat == 'csv':
            df = csv_stream_to_pandas(stream_data[0])  # Pass the first element of the stream_data list
            df.rename(columns={'Data Value': column_name}, inplace=True)
            df.set_index('Date/Time', inplace=True)
            df.index = pd.to_datetime(df.index)
            df_all = pd.concat([df_all, df], axis=1)
    df_all.columns = column_names
    current = datetime.now()
    current_td = timedelta(
        hours=current.hour,
        minutes=current.minute,
        seconds=current.second,
        microseconds=current.microsecond)
    to_hour = timedelta(hours=round(current_td.total_seconds() // 3600))
    tohour = datetime.combine(current, datetime.time(1)) + to_hour
    print(tohour)
    df_all = df_all.round().astype(int)

    df_all = df_all.resample('H', label='right', closed='right').mean()
    df_all = df_all[df_all.index < tohour]
    print(df_all)
    #upload_to_blob_storage(df_all, fromDateStr)
    return "HistoricalDB has completed."