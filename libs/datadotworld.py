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

from io import StringIO

import pandas as pd
import requests

BASE_DW_URL = 'https://api.data.world/v0'


class Datadotworld:
    def __init__(self, dw_token, dataset):
        self.headers = {'Authorization': f'Bearer {dw_token}'}
        self.dataset = dataset

    def fetch_dataset(self):
        url = f'{BASE_DW_URL}/datasets/{self.dataset}'
        r = requests.get(url, headers=self.headers)
        if r.status_code != 200:
            raise Exception(f"Dataset '{self.dataset}' doesn't exist, please create it: data.world/create-a-dataset")
        return r.json()

    def fetch_file(self, filename):
        url = f'{BASE_DW_URL}/file_download/{self.dataset}/{filename}'
        r = requests.get(url, headers=self.headers)
        if r.status_code == 404:
            return pd.DataFrame()
        elif r.status_code != 200:
            print(f'Failed to download {filename} from data.world')
            r.raise_for_status()
        return pd.read_csv(StringIO(r.text))

    def push(self, filename):
        url = f'{BASE_DW_URL}/uploads/{self.dataset}/files/{filename}'
        payload = open(filename, 'rb')
        r = requests.put(url, headers=self.headers, data=payload)
        if r.status_code != 200:
            print(f'Failed to upload {filename} to data.world')
            r.raise_for_status()
        payload.close()

    def update_summary(self, summary):
        url = f'{BASE_DW_URL}/datasets/{self.dataset}'
        body = {'summary': summary}
        r = requests.patch(url, headers=self.headers, json=body)
        if r.status_code != 200:
            print(f'Failed to update {self.dataset} with the summary: {summary}')
            r.raise_for_status()
