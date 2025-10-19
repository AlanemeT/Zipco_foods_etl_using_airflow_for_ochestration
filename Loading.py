#importing Necessary Libraries
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
# Data Loading into Azure Blob storage
def run_loading():
    # Loading the dataset
    data = pd.read_csv('/home/alaneme/airflow/zipco_food_dag/clean_data.csv')
    product = pd.read_csv('/home/alaneme/airflow/zipco_food_dag/product.csv')
    customer = pd.read_csv('/home/alaneme/airflow/zipco_food_dag/customer.csv')
    staff = pd.read_csv('/home/alaneme/airflow/zipco_food_dag/staff.csv')
    transaction = pd.read_csv('/home/alaneme/airflow/zipco_food_dag/transaction.csv')
    # Load the environment variables from the .env files
    load_dotenv()

    connect_str = os.getenv('AZURE_CONNECTION_STRING_VALUE')
    container_name = os.getenv('CONTAINER_NAME')

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Load data to Azure Blob Storage
    files = [
        (data, 'cleaneddata/clean_data.csv'),
        (product, 'processeddata/product.csv'),
        (customer, 'processeddata/customer.csv'),
        (staff, 'processeddata/staff.csv'),
        (transaction, 'processeddata/transaction.csv'),
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob Storage')
