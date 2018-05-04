import os
from datetime import datetime, timedelta

from libs.datadotworld import Datadotworld
from libs.mws import MWS

dw_token = os.environ['DW_TOKEN']
dataset = os.environ['DW_DATASET']  # TODO accept full urls (regex)

record_start_date = os.environ['RECORD_START_DATE']
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

now = datetime.now().date()
start_date = (now - timedelta(days=4)).isoformat()
end_date = now.isoformat()

for report in report_types:
    print(f"Working on {report['type']}")
    df = dw.fetch_from_dw(report['filename'])

    if df is None:
        df = mws.pull_historical_data(report['type'], record_start_date, end_date)
    else:
        df_new_data = mws.pull_from_mws(report['type'], start_date, end_date)
        if df_new_data is not None:
            df = df.append(df_new_data).drop_duplicates(subset=report['primary_key'], keep='last')
        else:
            print(f"No new data for {report['type']}")

    df.to_csv(report['filename'], index=False)
    dw.push_to_dw(report['filename'])

print('Done!')
