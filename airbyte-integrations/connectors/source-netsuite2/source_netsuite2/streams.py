#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime

import requests
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from requests_oauthlib import OAuth1
from source_netsuite2.constraints import INCREMENTAL_CURSOR, RECORD_PATH, REST_PATH



class Transactions(HttpStream, IncrementalMixin):
    def __init__(
    self,
    auth: OAuth1,
    base_url: str,
    start_datetime: str,
    window_in_days: int,
    ):
        self.base_url = base_url
        self.start_datetime = start_datetime
        self.window_in_days = window_in_days
        super().__init__(authenticator=auth)

    primary_key = "id"

    http_method = "POST"

    records_per_slice = 100

    request_limit = 1000

    raise_on_http_errors = True

    total_records = 0

    @property
    def state_checkpoint_interval(self) -> Optional[int]:
        return self.records_per_slice

    @property
    def url_base(self) -> str:
        return self.base_url

    @property
    def cursor_field(self) -> str:
        return INCREMENTAL_CURSOR

    def path(self, **kwargs) -> str:
        return REST_PATH + "query/v1/suiteql"

    @property
    def state(self) -> Mapping[str, Any]:
        if hasattr(self, "_state"):
            return self._state
        else:
            return {self.cursor_field: self.start_datetime}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        if hasattr(self, "_state"):
             self._state = {self.cursor_field: value[self.cursor_field]}
        else:
            self._state = {self.cursor_field: value[self.cursor_field]}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        resp = response.json()
        has_more = resp.get("hasMore")
        if has_more:
            return {"offset": resp["offset"] + resp["count"]}
        return None

    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = {"limit": self.request_limit}
        if next_page_token:
            params.update(**next_page_token)
        return params

    def request_headers(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
         return {"prefer": "transient", "Content-Type": "application/json"}


    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:
        current_cursor = stream_state.get(self.cursor_field, self.start_datetime)
        date_object_cursor = datetime.strptime(current_cursor, '%Y-%m-%dT%H:%M:%SZ')
        formated_cursor = date_object_cursor.strftime("%Y-%m-%d %H:%M:%S")
        return {
	        "q": "SELECT id, tranid, BUILTIN.DF(type) as type, to_char(lastModifiedDate, 'yyyy-mm-dd HH24:MI:SS') as lastModifiedDate FROM transaction as t WHERE  t.type IN ('SalesOrd','ItemRcpt','ItemShip','CashSale','PurchOrd') AND to_char(lastModifiedDate, 'yyyy-mm-dd HH24:MI:SS') > '" + formated_cursor + "' ORDER BY lastModifiedDate ASC"
        }


    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return response.json().get("items")

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            record_type = record.get("type").replace(" ", "").lower()
            url = self.base_url + RECORD_PATH + record_type + "/" + record.get("id")
            args = {"method": "GET", "url": url, "params": {"expandSubResources": True}}
            prep_req = self._session.prepare_request(requests.Request(**args))
            response = self._send_request(prep_req, request_kwargs={})
            if response.status_code == requests.codes.ok:
                transactionRecord = response.json()
                transactionRecord_with_type = {**transactionRecord, "type": record_type}
                self.logger.info(f"Fetched record {transactionRecord_with_type.get('id')} with datelastmodified {transactionRecord_with_type.get(self.cursor_field)}")
                self.state = transactionRecord_with_type
                self.logger.info(f"Current state is {self.state}")
                yield transactionRecord_with_type
            
    def fetch_record(self, record: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        record_type = record.get("type").replace(" ", "").lower()
        url = self.base_url + RECORD_PATH + record_type + "/" + record.get("id")
        args = {"method": "GET", "url": url, "params": {"expandSubResources": True}}
        prep_req = self._session.prepare_request(requests.Request(**args))
        response = self._send_request(prep_req, request_kwargs={})
        if response.status_code == requests.codes.ok:
            record = response.json()
            record_with_type = {**record, "type": record_type}
            return record_with_type





    

