import os
from azure.storage.blob import BlobServiceClient

account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
account_key = os.environ["AZURE_STORAGE_KEY"]

connection_string = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={account_name};"
    f"AccountKey={account_key};"
    f"EndpointSuffix=core.windows.net"
)

blob_service_client = BlobServiceClient.from_connection_string(
    connection_string
)

container_name = "gold"

blob_client = blob_service_client.get_blob_client(
    container=container_name,
    blob="customer_sales.csv"
)

with open("customer_sales.csv", "rb") as data:
    blob_client.upload_blob(
        data,
        overwrite=True
    )

print("File uploaded successfully done ok")