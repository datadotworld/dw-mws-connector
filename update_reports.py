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
        'filename': os.environ['ALL_ORDERS_FILENAME'],
        'type': '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_',
        'primary_key': 'amazon-order-id',
    }, {
        'filename': os.environ['CUSTOMER_RETURNS_FILENAME'],
        'type': '_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_',
        'primary_key': 'order-id',
    }]

dw = Datadotworld(dw_token, dataset)
mws = MWS(access_key, secret_key, seller_id, auth_token, marketplace_ids)

dataset = dw.fetch_dataset()
now = datetime.now().replace(microsecond=0)
last_run_time = None
pattern = r'bot ran ([0-9]*)'
tags = dataset['tags']
for i, t in enumerate(tags):
    match = re.match(pattern, t)
    if match:
        last_run_time = float(match.group(1))
        tags[i] = f'bot ran {int(now.timestamp())}'

if last_run_time:
    start_date = datetime.fromtimestamp(last_run_time)

for report in report_types:
    print(f"Working on {report['type']}")
    df = dw.fetch_file(report['filename'])

    if not last_run_time:
        df = mws.pull_historical_data(report['type'], start_date, now)
    else:
        df_new_data = mws.pull_from_mws(report['type'], start_date, now)
        if df_new_data is not None:
            df = df.append(df_new_data).drop_duplicates(subset=report['primary_key'], keep='last')
        else:
            print(f"No new data for {report['type']}")

    df.to_csv(report['filename'], index=False)
    dw.push(report['filename'])

dw.update_tags(tags)
print('Done!')
