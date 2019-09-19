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
import shutil
from datetime import datetime, timedelta

import pandas as pd

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

request_report_types = [
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
    }, ]

#Settlements Reports cannot be requested; only gotten-- i.e., they are prepared ahead of time, not generated on request
get_report_types = [
    {
        'title': 'Amazon Pay Settlements Report',
        'filename': os.environ.get('SETTLEMENTS_FILENAME', None),
        'initial_endpoint': '_GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2_',
        'update_endpoint': '_GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2_',
        'is_date_range_limited': True, #Endpoint can only pull last 3 months of data
    }
]

dw = Datadotworld(dw_token, dataset_slug)
mws = MWS(access_key, secret_key, seller_id, auth_token, marketplace_ids)

dataset = dw.fetch_dataset()
summary = dataset['summary'] if 'summary' in dataset else ''
now = datetime.utcnow().replace(microsecond=0)

pattern = r'(.*LAST BOT RUN:) (.*) UTC(.*)'
match = re.match(pattern, summary, flags=re.DOTALL)
# Pull date from last bot run
date_match = re.search(r'(\d{4}-\d{2}-\d{2})', summary)

current_time = now.isoformat()
if match:
    print(f"Found a 'last bot run' entry in the dataset ({dataset_slug}): {match[2]}")
    last_bot_run_date = datetime.strptime(date_match.group(), '%Y-%m-%d')
    summary = re.sub(pattern, fr"\1 {current_time} UTC\3", summary, flags=re.DOTALL)
else:
    print(f"No 'last bot run' entry found in the dataset ({dataset_slug})")
    summary = f"{summary}\n\nLAST BOT RUN: {current_time} UTC\n\n"

for report in request_report_types:
    def pull_reports_and_append(start_date):
        filenames = mws.pull_reports(report['initial_endpoint'], report['is_date_range_limited'], start_date, now)
        if filenames:
            for i, filename in enumerate(filenames):
                with open(filename, 'r') as f_in, open(report['filename'], 'a') as f_out:
                    for line in f_in:
                        if i > 0:
                            next(f_in)
                        f_out.write(line)

    if report['filename']:
        print(f"Working on {report['title']}")
        filesize = dw.fetch_file(report['filename'])
        if filesize > 0:
            print(f"{report['filename']} found in the dataset, size: {filesize} bytes")
        else:
            print(f"{report['filename']} not found in the dataset")

        if last_thirty_days and last_thirty_days.lower() in ('y', 'yes', 't', 'true'):
            print('LAST_THIRTY_DAYS flag is set')
            start_date = datetime.utcnow().replace(microsecond=0) - timedelta(days=30)
            pull_reports_and_append(start_date)
        elif not match or filesize == 0:
            print('Fetching data from Amazon from START_DATE')
            start_date = datetime.strptime(os.environ['START_DATE'], '%Y-%m-%d')
            pull_reports_and_append(start_date)
        else:
            # Intentionally overlap dates to make sure nothing is missed, requires de-duping
            start_date = datetime.strptime(match.group(2), '%Y-%m-%dT%H:%M:%S') - timedelta(days=5)

            print(f'Fetching data from Amazon from {start_date.isoformat()} to {current_time}')
            filenames = mws.pull_reports(report['update_endpoint'], report['is_date_range_limited'], start_date, now)
            if filenames:
                original_file = '.original'
                shutil.copyfile(report['filename'], original_file)
                original_list_of_keys = []
                indexes_to_remove = []
                for chunk in pd.read_csv(original_file, chunksize=1000):
                    original_list_of_keys += chunk[report['primary_key']].values.tolist()

                # Determine which lines need to be removed from the original list
                for filename in filenames:
                    df = pd.read_csv(filename, usecols=[report['primary_key']])
                    for key_from_new_file in df[report['primary_key']].drop_duplicates():
                        if key_from_new_file in original_list_of_keys:
                            for i, key_from_original_list in enumerate(original_list_of_keys):
                                if key_from_new_file == key_from_original_list:
                                    indexes_to_remove.append(i + 1)  # accounting for the header

                # Create a new file that excludes primary keys found in the new files
                with open(original_file) as f_in, open(report['filename'], 'w') as f_out:
                    for i, line in enumerate(f_in):
                        if i not in sorted(indexes_to_remove):
                            f_out.write(line)

                with open(report['filename'], 'a') as f_out:
                    for filename in filenames:
                        with open(filename, 'r') as f_in:
                            next(f_in)
                            for line in f_in:
                                f_out.write(line)
            else:
                print(f"No new data for {report['title']}")

        if os.path.exists(report['filename']) and os.path.getsize(report['filename']) > filesize:
            print(f"Pushing {report['filename']} to data.world")
            dw.push(report['filename'])


for report in get_report_types:
    def pull_reports_and_append(start_date):
        filenames = mws.get_reports(report['initial_endpoint'], report['is_date_range_limited'], start_date, now)
        #concat everything together
        if filenames:
            for i, filename in enumerate(filenames):
                with open(filename, 'r') as f_in, open(report['filename'], 'a') as f_out:
                    if i > 0:
                        next(f_in)
                    for line in f_in:
                        f_out.write(line)

    if report['filename']:
        start_date = datetime.strptime(os.environ['START_DATE'], '%Y-%m-%d')
        # if this has been run before, we only want to get data since the last time it was run
        if (last_bot_run_date > start_date):
            start_date = last_bot_run_date
        print(f"Working on {report['title']}")
        filesize = dw.fetch_file(report['filename'])

        if filesize > 0:
            print(f"{report['filename']} found in the dataset, size: {filesize} bytes")
        else:
            print(f"{report['filename']} not found in the dataset")

        if not match or filesize == 0:
            print(f'Fetching data from Amazon from {start_date.isoformat()} to {current_time}')
            pull_reports_and_append(start_date)
        else:
            print(f'Fetching data from Amazon from {start_date.isoformat()} to {current_time}')
            filenames = mws.get_reports(report['update_endpoint'], report['is_date_range_limited'], start_date, now)
            if filenames:
                original_file = '.original'
                shutil.copyfile(report['filename'], original_file)

                # Create a new file
                with open(original_file) as f_in, open(report['filename'], 'w') as f_out:
                    for i, line in enumerate(f_in):
                        f_out.write(line)

                with open(report['filename'], 'a') as f_out:
                    for filename in filenames:
                        with open(filename, 'r') as f_in:
                            next(f_in)
                            for line in f_in:
                                f_out.write(line)

            else:
                print(f"No new data for {report['title']}")
    if os.path.exists(report['filename']) and os.path.getsize(report['filename']) > 0:
        print(f"Pushing {report['filename']} to data.world")
    dw.push(report['filename'])

if not mws.error_occurred:
    print('Updating summary')
    dw.update_summary(summary)
print('Done!')
