import datetime
from datetime import datetime, timedelta
import http.client
import json
import time
import pandas as pd
import ssl
import certifi
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO








class NRGStreamApiCoalAvab:

    def __init__(self,username=None,password=None):
            self.username = 'suncor2'
            self.password = 'anisoTropical308'                
            self.server = 'api.nrgstream.com'        
            self.tokenPath = '/api/security/token'
            self.releasePath = '/api/ReleaseToken'
            self.tokenPayload = f'grant_type=password&username={self.username}&password={self.password}'
            self.tokenExpiry = datetime.now() - timedelta(seconds=60)
            self.accessToken = ""
                 

    def getToken(self):
            try:
                if self.isTokenValid() == False:                             
                    headers = {"Content-type": "application/x-www-form-urlencoded"}      
                    # Connect to API server to get a token
                    context = ssl.create_default_context(cafile=certifi.where())
                    conn = http.client.HTTPSConnection(self.server,context=context)
                    conn.request('POST', self.tokenPath, self.tokenPayload, headers)
                    res = conn.getresponse()                
                    res_code = res.code
                    # Check if the response is good
                    
                    if res_code == 200:
                        res_data = res.read()
                        # Decode the token into an object
                        jsonData = json.loads(res_data.decode('utf-8'))
                        self.accessToken = jsonData['access_token']                         
                        # Calculate new expiry date
                        self.tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])                        
                        #print('token obtained')
                        #print(self.accessToken)
                    else:
                        res_data = res.read()
                        print(res_data.decode('utf-8'))
                    conn.close()                          
            except Exception as e:
                print("getToken: " + str(e))
                # Release token if an error occured
                self.releaseToken()      
    def releaseToken(self):
            try:            
                headers = {}
                headers['Authorization'] = f'Bearer {self.accessToken}'            
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)
                conn.request('DELETE', self.releasePath, None, headers)  
                res = conn.getresponse()
                res_code = res.code
                if res_code == 200:   
                    # Set expiration date back to guarantee isTokenValid() returns false                
                    self.tokenExpiry = datetime.now() - timedelta(seconds=60)
                    #print('token released')            
            except Exception as e:
                print("releaseToken: " + str(e))
                        
    def isTokenValid(self):
            if self.tokenExpiry==None:
                return False
            elif datetime.now() >= self.tokenExpiry:            
                return False
            else:
                return True            

    def GetStreamDataByStreamId(self,streamIds, fromDate, toDate, dataFormat='csv', dataOption=''):
            stream_data = "" 
            # Set file format to csv or json            
            DataFormats = {}
            DataFormats['csv'] = 'text/csv'
            DataFormats['json'] = 'Application/json'
            
            try:                            
                for streamId in streamIds:            
                    # Get an access token            
                    self.getToken()    
                    if self.isTokenValid():
                        # Setup the path for data request. Pass dates in via function call
                        path = f'/api/StreamData/{streamId}'
                        if fromDate != '' and toDate != '':
                            path += f'?fromDate={fromDate.replace(" ", "%20")}&toDate={toDate.replace(" ", "%20")}'
                        if dataOption != '':
                            if fromDate != '' and toDate != '':
                                path += f'&dataOption={dataOption}'        
                            else:
                                path += f'?dataOption={dataOption}'        
                        
                        # Create request header
                        headers = {}            
                        headers['Accept'] = DataFormats[dataFormat]
                        headers['Authorization']= f'Bearer {self.accessToken}'
                        
                        # Connect to API server
                        context = ssl.create_default_context(cafile=certifi.where())
                        conn = http.client.HTTPSConnection(self.server,context=context)
                        conn.request('GET', path, None, headers)
                        res = conn.getresponse()        
                        res_code = res.code                    
                        if res_code == 200:   
                            try:
                                print(f'{datetime.now()} Outputing stream {path} res code {res_code}')
                                # output return data to a text file            
                                if dataFormat == 'csv':
                                    stream_data += res.read().decode('utf-8').replace('\r\n','\n') 
                                elif dataFormat == 'json':
                                    stream_data += json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                                conn.close()

                            except Exception as e:
                                print(str(e))            
                                self.releaseToken()
                                return None  
                        else:
                            print(str(res_code) + " - " + str(res.reason) + " - " + str(res.read().decode('utf-8')))
                        
                    self.releaseToken()   
                    # Wait 1 second before next request
                    time.sleep(1)
                return stream_data        
            except Exception as e:
                print(str(e))    
                self.releaseToken()
                return None
    def csvStreamToPandas(self, streamData):
        # split lines of return string from api
        streamData = streamData.split("\n")
        
        # remove empty elements from list
        streamData = [x for x in streamData if len(x) > 0] 
        
        # remove header data
        streamData = [x for x in streamData if x[0] != '#'] 
                     
        # split elements into lists of lists                     
        streamData = [x.split(",") for x in streamData] 
        
        # create dataframe
        df = pd.DataFrame(streamData[1:], columns=streamData[0]) 
        
        return df


def get_previous_hour():
    current_time = datetime.now()
    previous_hour = current_time - timedelta(hours=1)
    return previous_hour.strftime("%m/%d/%Y %H:%M")
def main(name: str) -> str:
    nrgStreamApi = NRGStreamApiCoalAvab('micoconnell@suncor.com', 'anisoTropical308')

    fromDateStr = get_previous_hour()
    toDateStr = get_previous_hour()

    dataFormat = 'csv'
    dataOption = ''

    getByStream = True
    stream_data_list = []

    if getByStream:
        #In order: CoalDCR,HydroDCR,GasDCR,EnergyDCR,SolarDCR,WindDCR,DualFuelDCR,OtherDCR,Dispatched Contingency Reserve -Gen
        column_names= ['CoalDCR','HydroDCR']
        streamIds = [80,79]                
#'HydroDCR','GasDCR','EnergyDCR','SolarDCR','WindDCR','DualFuelDCR','OtherDCR','DispatchedContingencyReserveGEN','DispatchedContingencyReserve','DispatchedContingencyReserveOther'
#,79,78,322680,322669,293674,322687,293675,228,227,2305

        df_all = pd.DataFrame()  # Create an empty DataFrame to store the data
        for stream_id, column_name in zip(streamIds, column_names):
            stream_data = nrgStreamApi.GetStreamDataByStreamId([stream_id], fromDateStr, toDateStr, dataFormat, dataOption)
            if dataFormat == 'csv':
                df = nrgStreamApi.csvStreamToPandas(stream_data)
                df.rename(columns={'Data Value': column_name}, inplace=True)  # Rename the 'Data Value' column
                df.set_index('Date/Time', inplace=True)  # Set the 'Date-Time' column as the index
                df.index = pd.to_datetime(df.index)  # Convert the index to a datetime object
                #df = df.resample('H').mean()  # Resample the data to hourly intervals and take the mean value
                df_all = pd.concat([df_all, df], axis=1)  # Add the DataFrame to the 'df_all' DataFrame and align them on their index
                
    df_all.columns = column_names
    import datetime as dt
    current = dt.datetime.now()
    current_td = dt.timedelta(
        hours = current.hour, 
        minutes = current.minute, 
        seconds = current.second, 
        microseconds = current.microsecond)
    to_hour = dt.timedelta(hours = round(current_td.total_seconds() // 3600))
    tohour = dt.datetime.combine(current, dt.time(1)) + to_hour
    print(tohour)
    df_all = df_all.round().astype(int)
    
    df_all = df_all.resample('H', label='right', closed='right').mean()
    df_all = df_all[df_all.index < tohour]
    #df_all = df_all.astype(int)
    #print(df_all.dtypes)
    #df_all = df_all.resample('60T').mean()
    #
    #df_all = df_all.resample('1H').mean()
        # Concatenate all DataFrames into one
         # Rename the 'Data Value' column
        #print(df_all)
    def upload_to_blob_storage(df_all,datetime_str):
    # Set your connection string here
        connection_string = "DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net"

        # Instantiate a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Specify the container name
        container_name = "supplycushionmetrics"

        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Create the container if it doesn't exist
        try:
            container_client.create_container()
        except:
            pass

        # Set the blob name with the format YYYY-MM-DD HH
        blob_name = f"blob.csv"
        csv_data = StringIO()
        df_all=df_all.to_csv()
        # Get the blob client
        blob_client = container_client.get_blob_client(blob_name)

        # Convert the dataframe to a CSV and upload it to the blob
        container_client = blob_client.upload_blob(df_all,overwrite=True)
    upload_to_blob_storage(df_all,fromDateStr)
    #df_all.columns = column_names
    return "HistoricalDB has completed."
