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
from datetime import datetime

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
        'filename': os.environ.get('ALL_ORDERS_FILENAME', None),
        'endpoint': '_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_',
        'primary_key': 'amazon-order-id',
        'is_date_range_limited': True,  # Endpoint can only pull 30 days of data
    }, {
        'title': 'FBA Returns Report',
        'filename': os.environ.get('CUSTOMER_RETURNS_FILENAME', None),
        'endpoint': '_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_',
        'primary_key': 'order-id',
        'is_date_range_limited': False,
    }, {
        'title': 'FBA Fulfilled Shipments Report',
        'filename': os.environ.get('FULFILLED_SHIPMENTS_FILENAME', None),
        'endpoint': '_GET_AMAZON_FULFILLED_SHIPMENTS_DATA_',
        'primary_key': 'order-id',
        'is_date_range_limited': True,
    }]

dw = Datadotworld(dw_token, dataset_slug)
mws = MWS(access_key, secret_key, seller_id, auth_token, marketplace_ids)

dataset = dw.fetch_dataset()
summary = dataset['summary'] if 'summary' in dataset else ''
now = datetime.utcnow().replace(microsecond=0)

pattern = r'(.*LAST BOT RUN:) (.*) UTC(.*)'
match = re.match(pattern, summary, flags=re.DOTALL)
if match:
    summary = re.sub(pattern, fr"\1 {now.isoformat()} UTC\3", summary, flags=re.DOTALL)
else:
    summary = f"{summary}\n\nLAST BOT RUN: {now.isoformat()} UTC\n\n"

for report in report_types:
    if report['filename']:
        print(f"Working on {report['title']}")
        df = mws.pull_report(report['endpoint'], report['is_date_range_limited'], start_date, now)

        if df is not None and not df.empty:
            df.to_csv(report['filename'], index=False)
            dw.push(report['filename'])

if not mws.error_occurred:
    dw.update_summary(summary)
print('Done!')
