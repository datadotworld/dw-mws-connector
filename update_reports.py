import os
import re
from datetime import datetime

from libs.datadotworld import Datadotworld
from libs.mws import MWS

dw_token = os.environ['DW_TOKEN']
dataset = os.environ['DW_DATASET']  # TODO accept full urls (regex)

start_date = os.environ['START_DATE']
access_key = os.environ['MWS_ACCESS_KEY']
secret_key = os.environ['MWS_SECRET_KEY']
seller_id = os.environ['MWS_SELLER_ID']
auth_token = os.environ['MWS_AUTH_TOKEN']
marketplace_ids = tuple(os.environ['MWS_MARKETPLACE_IDS'].replace(' ', '').split(','))

report_types = [
    {
        'title': 'All Orders Report',
        'filename': os.environ['ALL_ORDERS_FILENAME'],
        'initial_endpoint': '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_',
        'update_endpoint': '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_',
        'primary_key': 'amazon-order-id',
        'is_date_range_limited': True,  # Endpoint can only pull 30 days of data
    }, {
        'title': 'FBA Returns Report',
        'filename': os.environ['CUSTOMER_RETURNS_FILENAME'],
        'initial_endpoint': '_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_',
        'update_endpoint': '_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_',
        'primary_key': 'order-id',
        'is_date_range_limited': False,
    }]

dw = Datadotworld(dw_token, dataset)
mws = MWS(access_key, secret_key, seller_id, auth_token, marketplace_ids)

dataset = dw.fetch_dataset()
tags = dataset['tags']

last_run_time = None
now = datetime.now().replace(microsecond=0)

pattern = r'bot ran ([0-9]*)'
for i, t in enumerate(tags):
    match = re.match(pattern, t)
    if match:
        last_run_time = float(match.group(1))
        tags[i] = f'bot ran {int(now.timestamp())}'

if last_run_time:
    start_date = datetime.fromtimestamp(last_run_time)

for report in report_types:
    print(f"Working on {report['title']}")
    df = dw.fetch_file(report['filename'])

    if not last_run_time:
        df = mws.pull_report(report['initial_endpoint'], report['is_date_range_limited'], start_date, now)
    else:
        df_new_data = mws.pull_report(report['update_endpoint'], report['is_date_range_limited'], start_date, now)
        if df_new_data is not None:
            df = df.append(df_new_data)
        else:
            print(f"No new data for {report['title']}")

    df.drop_duplicates(subset=report['primary_key'], keep='last').to_csv(report['filename'], index=False)
    dw.push(report['filename'])

dw.update_tags(tags)
print('Done!')
