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

    def _get_report_list(self, max_count, types, start_date, end_date, next_token=None):
        filenames = list()
        r = self.reports_api.get_report_list(max_count=max_count,types=types,fromdate=start_date.isoformat(), todate=end_date.isoformat(), next_token=next_token)

        try:
            has_next = r.parsed['HasNext']
            if has_next == 'true':
                next_token = r.parsed['NextToken']
            if 'ReportInfo' in r.parsed:
                reports = r.parsed['ReportInfo']
                if isinstance(reports, list):
                    for report in reports:
                        report_id = report['ReportId']['value']
                        available_date = report['AvailableDate']['value']
                        raw_report = self._get_report(report_id) if report_id else None
                        if raw_report:
                            f = StringIO(raw_report)
                            filename = f'{available_date}.settlement.mws.tmp.csv'
                            pd.read_csv(f, sep='\t').to_csv(filename, index=False)
                            filenames.append(filename)
                            #sleep to avoid throttling
                            time.sleep(60)
                else:
                    report_id = reports['ReportId']['value']
                    available_date = reports['AvailableDate']['value']
                    raw_report = self._get_report(report_id) if report_id else None
                    if raw_report:
                        f = StringIO(raw_report)
                        filename = f'{available_date}.settlement.mws.tmp.csv'
                        pd.read_csv(f, sep='\t').to_csv(filename, index=False)
                        filenames.append(filename)

        except Exception:
            print(f'GetReportList failed for {types}')
            raise        

        if has_next == 'true':
            filenames.extend(self._get_report_list(types=types, next_token=next_token))
        return filenames

    def get_reports(self, report_type, is_date_range_limited, start_date, end_date):
        #first, get the report list
        filenames = self._get_report_list(max_count='100', types=report_type, start_date=start_date, end_date=end_date)
        return filenames

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
            report = r.original.decode(encoding='ISO-8859-1')
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
            if counter == 5:  # Timeout at 300 seconds
                raise Exception('Timeout while pulling from MWS')

            time.sleep(60)  # Delay here to prevent throttling & the reports are very slow to compile

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
            filename = f'{end_date}.mws.tmp'
            pd.read_csv(f, sep='\t').to_csv(filename, index=False)
            return filename
        else:
            return None

    def pull_reports(self, report_type, is_date_range_limited, start_date, end_date):
        filenames = list()
        if not is_date_range_limited or (end_date - start_date).total_seconds() <= self.thirty_days:
            filename = self._pull_single_report(report_type, start_date, end_date)
            if filename:
                filenames.append(filename)
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
                    filename = self._pull_single_report(report_type, start_date=dates[i], end_date=dates[i + 1])
                    if filename:
                        filenames.append(filename)
        return filenames
