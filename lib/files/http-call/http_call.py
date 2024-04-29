from datetime import datetime
from urllib3 import PoolManager
import boto3
from botocore.exceptions import NoCredentialsError
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

# Create a PoolManager instance for making HTTP requests
http = PoolManager()
s3_client = boto3.client('s3')


def get_data(http: PoolManager, url: str) -> bytes:
    # Make the HTTP request
    response = http.request('GET', url)
    if response.status != 200:
        raise Exception(f'Failed to fetch URL: {url}')

    return response.data


def store_data(s3_client: 'S3Client', data: bytes, bucket_name: str, key: str):
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
    except NoCredentialsError:
        raise Exception('Credentials not available for AWS S3')
    except Exception as e:
        print(e)


def handler(event, context):
    config = event['config']
    data = get_data(http, config['url'])
    now = datetime.utcnow().isoformat()
    key = f'{config["datasetProvider"]}/{config["datasetName"]}/{config["datasetName"]}-{now}.{config["datasetType"]}'
    store_data(
            s3_client,
            data,
            config['rawBucket'],
            key
    )

    return {
        'rawBucket': config['rawBucket'],
        'rawKey': key,
        'datasetProvider': config['datasetProvider'],
        'datasetName': config['datasetName'],
    }
