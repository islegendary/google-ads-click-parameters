import os
import json
import boto3
import snowflake.connector

# Environment variables for Snowflake
SF_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SF_USER = os.environ['SNOWFLAKE_USER']
SF_PASSWORD = os.environ['SNOWFLAKE_PASSWORD']
SF_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']
SF_DATABASE = os.environ['SNOWFLAKE_DATABASE']
SF_SCHEMA = os.environ['SNOWFLAKE_SCHEMA']

S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ.get('S3_KEY_PREFIX', 'click_performance/')
DDB_TABLE = os.environ['DYNAMO_TABLE_NAME']

TABLE_NAME = 'SEGMENT_EVENTS.GOOGLE_ADS_CLICK_PARAMETERS.CLICK_PERFORMANCE_REPORTS'

s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb').Table(DDB_TABLE)


def fetch_all_rows():
    ctx = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )
    cs = ctx.cursor()
    try:
        query = f"SELECT * FROM {TABLE_NAME}"
        cs.execute(query)
        for row in cs.fetchall():
            yield dict(zip([c[0] for c in cs.description], row))
    finally:
        cs.close()
        ctx.close()


def write_to_dynamodb(rows):
    for r in rows:
        ddb.put_item(Item=r)


def dump_to_s3(rows, key):
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(rows))


if __name__ == '__main__':
    rows = list(fetch_all_rows())
    key = f"{S3_PREFIX}initial_load.json"
    dump_to_s3(rows, key)
    write_to_dynamodb(rows)
    print(f"Wrote {len(rows)} records to S3 and DynamoDB")
