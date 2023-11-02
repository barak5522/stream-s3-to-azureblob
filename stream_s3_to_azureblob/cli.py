import os
from dotenv import load_dotenv
import boto3
from azure.storage.blob import BlobServiceClient
import threading

from stream_s3_to_azureblob import upload_to_blob_storage 
from stream_s3_to_azureblob.repository.client import MongoDB 

# Load environment variables from .env file
load_dotenv()
aws_access_key = os.getenv('S3_ACCESS_KEY')
aws_secret_key = os.getenv('S3_SECRET_KEY')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')
blob_storage_connection_string = os.getenv('BLOB_STORAGE_CONNECTION_STRING')
blob_storage_container_name = os.getenv('BLOB_STORAGE_CONTAINER_NAME')

s3_folder = os.getenv('S3_FOLDER')
file_extension = os.getenv('FILE_EXTENSION')

mongodb_uri = os.getenv('MONGODB_URI')
mongodb_db_name = os.getenv('MONGODB_DB_NAME')
mongodb_collection_name = os.getenv('MONGODB_COLLECTION_NAME')

pass_group_id_place = int(os.getenv('PASS_GROUP_ID_PLACE'))

db = MongoDB(mongodb_uri, mongodb_db_name)

statuses = {
    'pending': 'pending',
    'downloaded': 'downloaded',
    'failed': 'failed',
}
object_type = os.getenv('OBJECT_TYPE')

def get_pass_group_id_from_key(key):
    return key.split('/')[pass_group_id_place]

def transfer_new_object(s3, container_client, s3_key):
    print(f"Downloading {s3_key} from S3 to Azure Blob Storage")
    pass_group_id = get_pass_group_id_from_key(s3_key)
    blob_key = f'{pass_group_id}/{s3_key.split("/")[-1]}'

    _id = db.insert_one(mongodb_collection_name, {
        's3_key': s3_key,
        'blob_key': blob_key,
        'type': object_type,
        'pass_group_id': pass_group_id,
        'retries': 0,
        'status': statuses['pending'],
    })

    try: 
        upload_to_blob_storage(s3, container_client, s3_key, blob_key)
        db.update_one(mongodb_collection_name, { '_id': _id }, {'$set': {'status': statuses['downloaded']}})
        print(f"Successully downloaded {s3_key} from S3 to Azure Blob Storage")
    except Exception as e:
        db.update_one(mongodb_collection_name, { '_id': _id }, {'$set': {'status': statuses['failed']}})
        print(f"Error while downloading {s3_key}: {str(e)}")

def retries_transfer_objects(s3, container_client, obj):
    db.update_one(mongodb_collection_name, { '_id': obj['_id'] }, {'$set': {'status': statuses['pending'], 'retries': obj['retries'] + 1}})
    print(f"Downloading {obj['s3_key']} from S3 to Azure Blob Storage")
    try: 
        upload_to_blob_storage(s3, container_client, obj['s3_key'], obj['blob_key'])
        db.update_one(mongodb_collection_name, { '_id': obj['_id'] }, {'$set': {'status': statuses['downloaded']}})
        print(f"Successully downloaded {obj['s3_key']} from S3 to Azure Blob Storage")
    except Exception as e:
        db.update_one(mongodb_collection_name, { '_id': obj['_id'] }, {'$set': {'status': statuses['failed']}})
        print(f"Error while downloading {obj['s3_key']}: {str(e)}")

def main():
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    blob_service_client = BlobServiceClient.from_connection_string(blob_storage_connection_string)
    blob_service_client.max_single_put_size = 4*1024*1024 #4M
    blob_service_client.timeout = 60*20 # 20 mins
    container_client = blob_service_client.get_container_client(container=blob_storage_container_name)

    threads = []

    # download new objects
    last_inserted_object = db.find_one(mongodb_collection_name, { '$query':{ 'type': object_type }, '$orderby': { '_id': -1 }})
    print(last_inserted_object)
    objects = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_folder, StartAfter=last_inserted_object['s3_key'], MaxKeys=2)
    for obj in objects['Contents']:
        if obj['Key'].endswith(file_extension):
            # Create a thread for each file to upload to Azure Blob Storage
            thread = threading.Thread(target=transfer_new_object, args=(s3, container_client, obj['Key']))
            threads.append(thread)
            thread.start()
    
    # retries download failed objects
    failed_objects = db.find(mongodb_collection_name, { 'type': object_type, 'status': statuses['failed'], 'retries': { '$lt': 3 }})
    for obj in failed_objects:
        thread = threading.Thread(target=retries_transfer_objects, args=(s3, container_client, obj))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("All transfers completed.")