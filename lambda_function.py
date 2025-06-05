import os
import json
import logging
from datetime import datetime, timedelta
import psycopg2
import boto3

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

# --- Environment ---
SECRET_NAME = os.environ['GOOGLE_ADS_SECRET_NAME']
REGION = os.environ.get('AWS_REGION', 'us-east-1')
RDS_HOST = os.environ['RDS_HOST']
RDS_DB = os.environ['RDS_DB']
RDS_USER = os.environ['RDS_USER']
RDS_PASSWORD = os.environ['RDS_PASSWORD']
RDS_PORT = int(os.environ.get('RDS_PORT', '5432'))
S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ.get('S3_KEY_PREFIX', 'click_performance/')
DDB_TABLE = os.environ['DYNAMO_TABLE_NAME']
LOOKBACK_MIN = int(os.environ.get('INCREMENT_MINUTES', '5'))

# --- AWS Clients ---
sm = boto3.client('secretsmanager', region_name=REGION)
s3 = boto3.client('s3')
ddb = boto3.resource('dynamodb').Table(DDB_TABLE)

# --- Core Functions ---
def get_secret():
    return json.loads(sm.get_secret_value(SecretId=SECRET_NAME)['SecretString'])

def refresh_google_oauth(creds):
    cred_obj = Credentials(
        None,
        refresh_token=creds['refresh_token'],
        client_id=creds['client_id'],
        client_secret=creds['client_secret'],
        token_uri='https://oauth2.googleapis.com/token'
    )
    cred_obj.refresh(Request())
    if cred_obj.refresh_token and cred_obj.refresh_token != creds['refresh_token']:
        creds['refresh_token'] = cred_obj.refresh_token
        sm.put_secret_value(SecretId=SECRET_NAME,
                            SecretString=json.dumps(creds))
        logger.info('Stored new refresh token in Secrets Manager')
    return cred_obj

def build_client(creds):
    cred_obj = refresh_google_oauth(creds)
    return GoogleAdsClient(
        credentials=cred_obj,
        developer_token=creds['developer_token'],
        login_customer_id=creds['login_customer_id']
    )

def get_last_run():
    conn = None
    try:
        conn = psycopg2.connect(
            host=RDS_HOST, database=RDS_DB,
            user=RDS_USER, password=RDS_PASSWORD, port=RDS_PORT
        )
        with conn.cursor() as cur:
            cur.execute("SELECT last_timestamp FROM gclid_tracking ORDER BY last_timestamp DESC LIMIT 1")
            row = cur.fetchone()
            if not row or not row[0]:
                return (datetime.utcnow() - timedelta(minutes=LOOKBACK_MIN)).isoformat()
            return row[0].isoformat()
    except Exception as e:
        logger.error(f"Error reading last timestamp from RDS: {e}")
        raise
    finally:
        if conn:
            conn.close()

def set_last_run(ts):
    conn = None
    try:
        conn = psycopg2.connect(
            host=RDS_HOST, database=RDS_DB,
            user=RDS_USER, password=RDS_PASSWORD, port=RDS_PORT
        )
        with conn.cursor() as cur:
            cur.execute("UPDATE gclid_tracking SET last_timestamp = %s", (ts,))
            conn.commit()
    except Exception as e:
        logger.error(f"Error updating last timestamp in RDS: {e}")
        raise
    finally:
        if conn:
            conn.close()

def list_customer_ids(client):
    service = client.get_service("CustomerService")
    response = service.list_accessible_customers()
    return [res.replace("customers/", "") for res in response.resource_names]

def query_clicks(client, customer_id, start_ts, end_ts):
    service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT click_view.gclid, campaign.id, ad_group_ad.ad.id,
               click_view.ad_network_type, segments.date_time
        FROM click_view
        WHERE segments.date_time BETWEEN '{start_ts}' AND '{end_ts}'
    """
    results = []
    try:
        for batch in service.search_stream(customer_id=customer_id, query=query):
            for row in batch.results:
                results.append({
                    'gclid': row.click_view.gclid,
                    'campaign_id': row.campaign.id,
                    'creative_id': row.ad_group_ad.ad.id,
                    'ad_network_type': row.click_view.ad_network_type.name,
                    'timestamp': row.segments.date_time.value,
                    'customer_id': customer_id,
                })
    except GoogleAdsException as exc:
        logger.warning(f"Google Ads error for customer {customer_id}: {exc}")
    return results

def write_to_dynamodb(items):
    with ddb.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

# --- Lambda Entrypoint ---
def lambda_handler(event, context):
    creds = get_secret()
    client = build_client(creds)

    start_ts = get_last_run()
    end_ts = datetime.utcnow().isoformat()

    logger.info(f"Running for time window: {start_ts} -> {end_ts}")
    all_data = []

    try:
        customer_ids = list_customer_ids(client)
    except RefreshError:
        logger.info("OAuth token expired, reloading credentials")
        creds = get_secret()
        client = build_client(creds)
        customer_ids = list_customer_ids(client)

    for cid in customer_ids:
        logger.info(f"Processing customer {cid}")
        try:
            rows = query_clicks(client, cid, start_ts, end_ts)
        except RefreshError:
            logger.info("OAuth token expired during query, reloading credentials")
            creds = get_secret()
            client = build_client(creds)
            rows = query_clicks(client, cid, start_ts, end_ts)
        all_data.extend(rows)

    if not all_data:
        set_last_run(end_ts)
        return {'statusCode': 204, 'body': 'No data'}

    ts = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
    key = f"{S3_PREFIX}clicks_{ts}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(all_data))

    write_to_dynamodb(all_data)
    set_last_run(end_ts)

    return {
        'statusCode': 200,
        'body': f"Wrote {len(all_data)} records from {start_ts} to {end_ts}"
    }
