import os
from datetime import datetime, timedelta

from libs.datadotworld import Datadotworld
from libs.mws import MWS

api_key = os.environ['DW_TOKEN']
dataset = os.environ['DW_DATASET']

access_key = os.environ['MWS_ACCESS_KEY']
secret_key = os.environ['MWS_SECRET_KEY']
seller_id = os.environ['MWS_SELLER_ID']
auth_token = os.environ['MWS_AUTH_TOKEN']
marketplace_id = os.environ['MWS_MARKETPLACE_ID']

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

dw = Datadotworld(api_key, dataset)
mws = MWS(access_key, secret_key, seller_id, auth_token, marketplace_id)

now = datetime.now()
start_date = f"{(now - timedelta(days=1)).strftime('%Y-%m-%d')}T00:00:00+00:00"
end_date = f"{now.strftime('%Y-%m-%d')}T00:00:00+00:00"

for report in report_types:
    df_new = mws.pull_from_mws(report['type'], start_date, end_date)

    if df_new:
        df = dw.fetch_from_dw(report['filename'])
        df.update(df_new)

        df.to_csv(report['filename'], index=False)
        dw.push_to_dw(report['filename'])
    else:
        print('No new data')
