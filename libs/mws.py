import time
from io import StringIO

import mws
import pandas as pd


class MWS:
    def __init__(self, access_key, secret_key, seller_id, auth_token, marketplace_id):
        self.marketplace_id = marketplace_id

        self.reports_api = mws.Reports(
            access_key=access_key,
            secret_key=secret_key,
            account_id=seller_id,
            auth_token=auth_token,
        )

    def request_report(self, report_type, start_date, end_date):
        r = self.reports_api.request_report(report_type=report_type,
                                            start_date=start_date,
                                            end_date=end_date,
                                            marketplaceids=self.marketplace_id)

        try:
            request_id = r.parsed['ReportRequestInfo']['ReportRequestId']['value']
        except Exception:
            print('RequestReport failed')
            raise
        return request_id

    def get_report_request_list(self, request_id):
        r = self.reports_api.get_report_request_list(requestids=request_id)

        report_id = None
        try:
            report_request_info = r.parsed['ReportRequestInfo']
            status = report_request_info['ReportProcessingStatus']['value']
            if 'GeneratedReportId' in report_request_info:
                report_id = report_request_info['GeneratedReportId']['value']
        except Exception:
            print('GetReportRequestList failed')
            raise
        return status, report_id

    def get_report(self, report_id):
        r = self.reports_api.get_report(report_id)

        try:
            report = r.original.decode()
        except Exception:
            print('GetReport failed')
            raise
        return report

    def pull_from_mws(self, report_type, start_date, end_date):
        request_id = self.request_report(report_type=report_type,
                                         start_date=start_date,
                                         end_date=end_date)
        counter = 0
        while True:
            time.sleep(10)
            if counter > 12:  # Timeout after 120 seconds
                raise Exception('Timeout while pulling from MWS')

            status, report_id = self.get_report_request_list(request_id)
            if status in ('_DONE_', '_DONE_NO_DATA_'):
                break
            elif status == '_CANCELLED_':
                print(request_id)
                raise Exception('Request was cancelled')
            counter += 1

        raw_report = self.get_report(report_id) if report_id else None

        if raw_report:
            f = StringIO(raw_report)
            return pd.read_csv(f, sep='\t')
        else:
            return None
