import json
import os
import re
import ssl
import boto3
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef

s3_client = boto3.client('s3')
secretsmanager = boto3.client('secretsmanager')
rds_secret_name = os.environ['RDS_SECRET_NAME']

ssl_context = ssl.create_default_context()
ssl_context.verify_mode = ssl.CERT_REQUIRED
ssl_context.load_verify_locations("/opt/python/global-bundle.pem")


def get_data(s3_client: 'S3Client', bucket: str, key: str) -> 'GetObjectOutputTypeDef':
    return s3_client.get_object(Bucket=bucket, Key=key)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.drop(["eaRegionName", "floodArea"], axis=1)
    string_cols = ['@id', 'description', 'eaAreaName', 'floodAreaID', 'message', 'severity']
    bool_cols = ['isTidal']
    timestamp_cols = ['timeMessageChanged', 'timeRaised', 'timeSeverityChanged']
    int_cols = ['severityLevel']

    df[string_cols] = df[string_cols].astype('string')
    df[bool_cols] = df[bool_cols].astype(bool)
    df[timestamp_cols] = df[timestamp_cols].apply(pd.to_datetime, format='%Y-%m-%dT%H:%M:%S')
    df[int_cols] = df[int_cols].astype(int)
    df['description'] = df['description'].apply(lambda x: x.strip())
    df['message'] = df['message'].apply(lambda x: x.strip())
    df.columns = df.columns.to_series().apply(  # type: ignore
            lambda x: re.sub('([a-z0-9])([A-Z])', r'\1_\2', x).lower()
    )

    return df


def handler(event, context) -> dict:
    # Get DB Credentials and connect
    get_secret_value_response = secretsmanager.get_secret_value(SecretId=rds_secret_name)
    db = json.loads(get_secret_value_response['SecretString'])
    escaped_password = quote_plus(db['password'])
    connection_string = f"postgresql://{db['username']}:{escaped_password}@{db['host']}:{db['port']}/{db['database']}"
    connect_args = {
        'sslmode': 'require',
        'sslrootcert': '/opt/python/global-bundle.pem'
    }
    engine = create_engine(connection_string, connect_args=connect_args)

    # Get raw data and run transformations
    sm_input = event['Payload']
    dataset_provider = sm_input['datasetProvider'].lower()
    dataset_name = sm_input['datasetName'].lower()
    data = get_data(s3_client, sm_input['rawBucket'], sm_input['rawKey'])['Body'].read()
    df = pd.DataFrame(json.loads(data)['items'])
    transformed_df = transform(df)

    table = f'{dataset_provider}_{dataset_name}'
    transformed_df.to_sql(
            table,
            engine,
            schema="dataset",
            index=False,
            if_exists="replace",
            )

    return {
        'rowsProcessed': len(df)
    }
