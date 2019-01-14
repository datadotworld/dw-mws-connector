# dw-mws-connector
# Copyright 2018 data.world, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# data.world, Inc.(http://data.world/).

import os
import re
from datetime import datetime, timedelta

from libs.datadotworld import Datadotworld
from libs.mws import MWS

dw_token = os.environ['DW_TOKEN']
dataset_slug = os.environ['DW_DATASET_SLUG']

access_key = os.environ['MWS_ACCESS_KEY']
secret_key = os.environ['MWS_SECRET_KEY']
seller_id = os.environ['MWS_SELLER_ID']
auth_token = os.environ['MWS_AUTH_TOKEN']
marketplace_ids = tuple(os.environ['MWS_MARKETPLACE_IDS'].replace(' ', '').split(','))

last_thirty_days = os.environ.get('LAST_THIRTY_DAYS', None)

report_types = [
    {
        'title': 'All Orders Report',
        'filename': os.environ.get('ALL_ORDERS_FILENAME', None),
        'initial_endpoint': '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_',
        'update_endpoint': '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_',
        'primary_key': 'amazon-order-id',
        'is_date_range_limited': True,  # Endpoint can only pull 30 days of data
    }, {
        'title': 'FBA Returns Report',
        'filename': os.environ.get('CUSTOMER_RETURNS_FILENAME', None),
        'initial_endpoint': '_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_',
        'update_endpoint': '_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_',
        'primary_key': 'order-id',
        'is_date_range_limited': False,
    }, {
        'title': 'FBA Fulfilled Shipments Report',
        'filename': os.environ.get('FULFILLED_SHIPMENTS_FILENAME', None),
        'initial_endpoint': '_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_',
        'update_endpoint': '_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_',
        'primary_key': 'amazon-order-id',
        'is_date_range_limited': True,
    }]

dw = Datadotworld(dw_token, dataset_slug)
mws = MWS(access_key, secret_key, seller_id, auth_token, marketplace_ids)

dataset = dw.fetch_dataset()
summary = dataset['summary'] if 'summary' in dataset else ''
now = datetime.utcnow().replace(microsecond=0)

pattern = r'(.*LAST BOT RUN:) (.*) UTC(.*)'
match = re.match(pattern, summary, flags=re.DOTALL)
current_time = now.isoformat()
if match:
    print(f"Found a 'last bot run' entry in the dataset ({dataset_slug}): {match[2]}")
    summary = re.sub(pattern, fr"\1 {current_time} UTC\3", summary, flags=re.DOTALL)
else:
    print(f"No 'last bot run' entry found in the dataset ({dataset_slug})")
    summary = f"{summary}\n\nLAST BOT RUN: {current_time} UTC\n\n"

for report in report_types:
    if report['filename']:
        print(f"Working on {report['title']}")
        df = dw.fetch_file(report['filename'])
        if df is None:
            print('No existing data found in the dataset')

        if last_thirty_days and last_thirty_days.lower() in ('y', 'yes', 't', 'true'):
            print('LAST_THIRTY_DAYS flag is set')
            start_date = datetime.utcnow().replace(microsecond=0) - timedelta(days=30)
            df = mws.pull_report(report['initial_endpoint'], report['is_date_range_limited'], start_date, now)
            df['script-run-time'] = match.group(2)
        elif not match or df is None:
            print('Fetching data from Amazon from START_DATE')
            start_date = datetime.strptime(os.environ['START_DATE'], '%Y-%m-%d')
            df = mws.pull_report(report['initial_endpoint'], report['is_date_range_limited'], start_date, now)
            df['script-run-time'] = match.group(2)
        else:
            # Intentionally overlap dates to make sure nothing is missed, requires de-duping
            start_date = datetime.strptime(match.group(2), '%Y-%m-%dT%H:%M:%S') - timedelta(days=5)

            print(f'Fetching data from Amazon from {start_date.isoformat()} to {current_time}')
            df_new_data = mws.pull_report(report['update_endpoint'], report['is_date_range_limited'], start_date, now)
            if df_new_data is not None:
                # For compatibility with older implementations
                if 'script-run-time' not in df.columns:
                    df['script-run-time'] = match.group(2)

                df_new_data['script-run-time'] = current_time
                df = df.append(df_new_data, ignore_index=True)

                # Remove duplicates by keeping entries from newest 'script-run-time' group
                df_maxes = df.groupby(report['primary_key'], sort=False)['script-run-time'].transform(max)
                df = df.loc[df['script-run-time'] == df_maxes]
            else:
                print(f"No new data for {report['title']}")

        if df is not None and not df.empty:
            df.to_csv(report['filename'], index=False)

            print(f"Pushing {report['filename']} to data.world")
            dw.push(report['filename'])

if not mws.error_occurred:
    print('Updating summary')
    dw.update_summary(summary)
print('Done!')
