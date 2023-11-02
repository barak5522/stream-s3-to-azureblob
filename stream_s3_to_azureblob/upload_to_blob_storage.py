import sys
import os
import uuid
from dotenv import load_dotenv
from azure.storage.blob import ContainerClient, BlobBlock

# Load environment variables from .env file
load_dotenv()
s3_bucket_name = os.getenv('S3_BUCKET_NAME')

# Define a function to stream data from S3 to Azure Blob Storage
def upload_to_blob_storage(s3, blob_container_client: ContainerClient, s3_key, blob_key):
    try:
        # Upload to Azure Blob Storage using streaming
        azure_blob_client = blob_container_client.get_blob_client(blob_key)

        # Stream from S3
        s3_response = s3.get_object(Bucket=s3_bucket_name, Key=s3_key)
        s3_data_stream = s3_response['Body']
        
        # upload data
        block_list=[]
        chunk_size=1024*1024*4
        while s3_data_stream:
            blk_id = str(uuid.uuid4())
            azure_blob_client.stage_block(block_id=blk_id,data=read_data) 
            block_list.append(BlobBlock(block_id=blk_id))
        azure_blob_client.commit_block_list(block_list)
        
        print(f"Streamed {s3_key} from S3 to Azure Blob Storage ({s3_key})")
    except Exception as e:
        raise e

sys.modules[__name__] = upload_to_blob_storage