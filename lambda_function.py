"""Lambda function for exporting recent Google Ads clicks.

The function queries all accessible customer accounts for click view data,
writes the records to DynamoDB and S3, and stores the last run timestamp in an
RDS table. OAuth credentials are refreshed automatically and persisted back to
Secrets Manager when a new refresh token is returned.
"""

import os
import json
import logging
from datetime import datetime, timedelta
import psycopg2
import boto3
import yaml

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as GoogleCredentials

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

# --- Secrets ---
def get_secret() -> dict:
    """Load the OAuth and Ads API credentials from Secrets Manager."""
    return json.loads(sm.get_secret_value(SecretId=SECRET_NAME)['SecretString'])

def update_secret_with_new_refresh_token(new_token: str) -> None:
    """Persist a new refresh token back to Secrets Manager."""
    secret = json.loads(sm.get_secret_value(SecretId=SECRET_NAME)['SecretString'])
    secret['refresh_token'] = new_token
    sm.put_secret_value(SecretId=SECRET_NAME, SecretString=json.dumps(secret))
    logger.info("🔁 Updated Secrets Manager with new refresh token.")

# --- Google Ads Client with Refresh Handling ---
def build_client_with_refresh(creds: dict) -> GoogleAdsClient:
    """Return an authenticated GoogleAdsClient refreshing OAuth as needed."""
    token_creds = GoogleCredentials(
        token=None,
        refresh_token=creds['refresh_token'],
        token_uri='https://oauth2.googleapis.com/token',
        client_id=creds['client_id'],
        client_secret=creds['client_secret'],
        scopes=["https://www.googleapis.com/auth/adwords"]
    )

    request = Request()
    token_creds.refresh(request)

    new_refresh_token = token_creds.refresh_token
    if new_refresh_token and new_refresh_token != creds['refresh_token']:
        update_secret_with_new_refresh_token(new_refresh_token)

    config_path = '/tmp/google-ads.yaml'
    with open(config_path, 'w') as f:
        yaml.dump({
            'developer_token': creds['developer_token'],
            'client_id': creds['client_id'],
            'client_secret': creds['client_secret'],
            'refresh_token': new_refresh_token,
            'login_customer_id': creds['login_customer_id'],
        }, f)

    return GoogleAdsClient.load_from_storage(config_path)

# --- RDS Timestamp Tracking ---
def get_last_run() -> str:
    """Return the timestamp of the previous successful execution."""
    with psycopg2.connect(
        host=RDS_HOST,
        database=RDS_DB,
        user=RDS_USER,
        password=RDS_PASSWORD,
        port=RDS_PORT,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_timestamp FROM gclid_tracking "
                "ORDER BY last_timestamp DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return (datetime.utcnow() - timedelta(minutes=LOOKBACK_MIN)).isoformat()
            return row[0].isoformat()

def set_last_run(ts: str) -> None:
    """Persist the latest execution timestamp to RDS."""
    with psycopg2.connect(
        host=RDS_HOST,
        database=RDS_DB,
        user=RDS_USER,
        password=RDS_PASSWORD,
        port=RDS_PORT,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE gclid_tracking SET last_timestamp = %s", (ts,))
            conn.commit()

# --- Google Ads Operations ---
def list_customer_ids(client: GoogleAdsClient) -> list:
    """Return the customer IDs accessible to the credentials."""
    service = client.get_service("CustomerService")
    response = service.list_accessible_customers()
    return [res.replace("customers/", "") for res in response.resource_names]

def query_clicks(
    client: GoogleAdsClient, customer_id: str, start_ts: str, end_ts: str
) -> list:
    """Fetch click view rows for a single customer within the time window."""
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

# --- Output Handlers ---
def write_to_dynamodb(items: list) -> None:
    """Bulk write click records to DynamoDB."""
    with ddb.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

# --- Lambda Entrypoint ---
def lambda_handler(event, context):
    """Entry point for AWS Lambda."""
    creds = get_secret()
    client = build_client_with_refresh(creds)

    start_ts = get_last_run()
    end_ts = datetime.utcnow().isoformat()

    logger.info(f"Running for window: {start_ts} → {end_ts}")
    all_data = []

    for cid in list_customer_ids(client):
        logger.info(f"📡 Querying customer: {cid}")
        data = query_clicks(client, cid, start_ts, end_ts)
        all_data.extend(data)

    if not all_data:
        logger.info("No data returned.")
        set_last_run(end_ts)
        return {'statusCode': 204, 'body': 'No data'}

    ts = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
    key = f"{S3_PREFIX}clicks_{ts}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(all_data))
    logger.info(f"Wrote {len(all_data)} records to s3://{S3_BUCKET}/{key}")

    write_to_dynamodb(all_data)
    set_last_run(end_ts)

    return {
        'statusCode': 200,
        'body': f"Wrote {len(all_data)} records from {start_ts} to {end_ts}"
    }
