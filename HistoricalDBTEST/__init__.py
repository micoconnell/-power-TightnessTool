import logging
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
import numpy as np

def main(name: str) -> str:
    df = pd.DataFrame(np.random.randint(0, 100, size=(10, 10)), columns=['col_' + str(i+1) for i in range(10)])

    # Save the dataframe to a CSV file
    csv_data = df.to_csv(index=False)

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
    blob_name = f"{datetime.utcnow().strftime('%Y-%m-%d %H')}.csv"

    # Get the blob client
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the CSV data to the blob
    blob_client.upload_blob(csv_data.encode(), overwrite=True)

    return f"Hello {name}!"