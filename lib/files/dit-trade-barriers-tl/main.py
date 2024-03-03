import json
import os
import boto3
import pandas as pd
from sqlalchemy import create_engine, types
from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from urllib.parse import quote_plus
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef

s3_client = boto3.client('s3')
secretsmanager = boto3.client('secretsmanager')
rds_secret_name = os.environ['RDS_SECRET_NAME']


def get_data(s3_client: 'S3Client', bucket: str, key: str) -> 'GetObjectOutputTypeDef':
    return s3_client.get_object(Bucket=bucket, Key=key)


def transform(df) -> pd.DataFrame:
    df = df.copy()
    string_cols = ['id', 'title', 'summary', 'trading_bloc', 'location', 'categories']
    bool_cols = ['is_resolved', 'caused_by_trading_bloc']
    date_cols = ['status_date']
    timestamp_cols = ['last_published_on', 'reported_on']
    df['country'] = df['country'].apply(json.dumps)
    # Flatten the sectors column into a list
    df['sectors'] = df['sectors'].apply(lambda row: [item['name'] for item in row])

    df[string_cols] = df[string_cols].astype('string')
    df[bool_cols] = df[bool_cols].astype(bool)
    df[date_cols] = df[date_cols].apply(pd.to_datetime, format='%Y-%m-%d')
    df[timestamp_cols] = df[timestamp_cols].apply(pd.to_datetime, format='%Y-%m-%dT%H:%M:%S.%fZ')
    df = df.drop(["trading_bloc", "categories"], axis=1)

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

    sm_input = event['Payload']
    data = get_data(s3_client, sm_input['rawBucket'], sm_input['rawKey'])['Body'].read()
    df = pd.DataFrame(json.loads(data)['barriers'])
    transformed_df = transform(df)

    # Write to database
    table = f'{sm_input["datasetProvider"]}_{sm_input["datasetName"]}'
    transformed_df.to_sql(
            table,
            engine,
            schema="dataset",
            index=False,
            if_exists="replace",
            dtype={'country': types.JSON, 'sectors': ARRAY(TEXT)}  # type: ignore
            )

    return {
        'rowsProcessed': len(df)
    }
