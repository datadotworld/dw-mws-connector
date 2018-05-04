from io import StringIO

import pandas as pd
import requests

BASE_DW_URL = 'https://api.data.world/v0'


class Datadotworld:
    def __init__(self, dw_token, dataset):
        self.headers = {'Authorization': f'Bearer {dw_token}'}
        self.dataset = dataset

    def fetch_from_dw(self, filename):
        url = f'{BASE_DW_URL}/file_download/{self.dataset}/{filename}'

        r = requests.get(url, headers=self.headers)
        if r.status_code != 200:
            print(f'Failed to download {filename} from data.world')
            r.raise_for_status()
        # elif :  # TODO specific error that denotes file doesn't exist
        #     return None

        return pd.read_csv(StringIO(r.text))

    def push_to_dw(self, filename):
        url = f'{BASE_DW_URL}/uploads/{self.dataset}/files/{filename}'
        payload = open(filename, 'rb')

        r = requests.put(url, headers=self.headers, data=payload)
        if r.status_code != 200:
            print(f'Failed to upload {filename} to data.world')
            r.raise_for_status()

        payload.close()
