import os
import re
from datetime import datetime

import pandas as pd
from libs.datadotworld import Datadotworld
from libs.mws import MWS

dw_token = os.environ['DW_TOKEN']
dataset_slug = os.environ['DW_DATASET_SLUG']

start_date = datetime.strptime(os.environ['START_DATE'], '%Y-%m-%d')
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

dw = Datadotworld(dw_token, dataset_slug)
mws = MWS(access_key, secret_key, seller_id, auth_token, marketplace_ids)

dataset = dw.fetch_dataset()
summary = dataset['summary'] if 'summary' in dataset else ''
now = datetime.now().replace(microsecond=0)

pattern = r'(.*LAST BOT RUN:) ([0-9]*)\.(.*)'
match = re.match(pattern, summary, flags=re.DOTALL)
if match:
    start_date = datetime.fromtimestamp(float(match.group(2)))
    summary = re.sub(pattern, fr"\1 {int(now.timestamp())}.\3", summary, flags=re.DOTALL)
else:
    summary = f"{summary}\n\nLAST BOT RUN: {int(now.timestamp())}."

for report in report_types:
    print(f"Working on {report['title']}")
    df = pd.DataFrame()

    if not match or df is None:
        df = mws.pull_report(report['initial_endpoint'], report['is_date_range_limited'], start_date, now)
    else:
        df = dw.fetch_file(report['filename'])
        df_new_data = mws.pull_report(report['update_endpoint'], report['is_date_range_limited'], start_date, now)
        if df_new_data is not None:
            df = df.append(df_new_data, ignore_index=True)
        else:
            print(f"No new data for {report['title']}")

    if df is not None and not df.empty:
        df.drop_duplicates(subset=report['primary_key'], keep='last').to_csv(report['filename'], index=False)
        dw.push(report['filename'])

dw.update_summary(summary)
print('Done!')
