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

import time
from datetime import timedelta
from io import StringIO

import mws as mws_api
import pandas as pd


class MWS:
    def __init__(self, access_key, secret_key, seller_id, auth_token, marketplace_ids):
        self.marketplace_ids = marketplace_ids
        self.reports_api = mws_api.Reports(
            access_key=access_key,
            secret_key=secret_key,
            account_id=seller_id,
            auth_token=auth_token,
        )
        self.thirty_days = 2592000.0  # Seconds
        self.error_occurred = False

    def _request_report(self, report_type, start_date, end_date):
        r = self.reports_api.request_report(report_type=report_type,
                                            start_date=start_date.isoformat(),
                                            end_date=end_date.isoformat(),
                                            marketplaceids=self.marketplace_ids)

        try:
            request_id = r.parsed['ReportRequestInfo']['ReportRequestId']['value']
        except Exception:
            print(f'RequestReport failed for {report_type}')
            raise
        return request_id

    def _get_report_request_list(self, request_id):
        r = self.reports_api.get_report_request_list(requestids=request_id)

        report_id = None
        try:
            report_request_info = r.parsed['ReportRequestInfo']
            status = report_request_info['ReportProcessingStatus']['value']
            if 'GeneratedReportId' in report_request_info:
                report_id = report_request_info['GeneratedReportId']['value']
        except Exception:
            print(f'GetReportRequestList failed for {request_id}')
            raise
        return status, report_id

    def _get_report(self, report_id):
        r = self.reports_api.get_report(report_id)

        try:
            report = r.original.decode()
        except Exception:
            print(f'GetReport failed for {report_id}')
            raise
        return report

    def _pull_single_report(self, report_type, start_date, end_date):
        request_id = self._request_report(report_type=report_type,
                                          start_date=start_date,
                                          end_date=end_date)
        counter = 0
        while True:
            time.sleep(30)  # Delay here to prevent throttling & the reports are very slow to compile
            if counter > 4:  # Timeout after 120 seconds
                raise Exception('Timeout while pulling from MWS')

            status, report_id = self._get_report_request_list(request_id)
            if status in ('_DONE_', '_DONE_NO_DATA_'):
                break
            elif status == '_CANCELLED_':
                print(f'Request {request_id} was cancelled, skipping report')
                self.error_occurred = True
                break
            counter += 1

        raw_report = self._get_report(report_id) if report_id else None
        if raw_report:
            f = StringIO(raw_report)
            return pd.read_csv(f, sep='\t')
        else:
            return None

    def pull_report(self, report_type, is_date_range_limited, start_date, end_date):
        df = pd.DataFrame()

        if not is_date_range_limited or (end_date - start_date).total_seconds() <= self.thirty_days:
            df_tmp = self._pull_single_report(report_type, start_date, end_date)
            df = df.append(df_tmp, ignore_index=True)
        else:
            dates = [start_date]

            while True:
                dt = dates[-1] + timedelta(days=30)
                if dt < end_date:
                    dates.append(dt)
                else:
                    dates.append(end_date)
                    break

            for i in range(len(dates)):
                if i < len(dates) - 1:
                    df_tmp = self._pull_single_report(report_type, start_date=dates[i], end_date=dates[i + 1])
                    df = df.append(df_tmp, ignore_index=True)
        return df
