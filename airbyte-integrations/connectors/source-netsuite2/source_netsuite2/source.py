#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime

import requests
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from requests_oauthlib import OAuth1
from source_netsuite2.constraints import CUSTOM_INCREMENTAL_CURSOR, INCREMENTAL_CURSOR, RECORD_PATH, REST_PATH

# Basic full refresh stream
class Netsuite2Stream(HttpStream, ABC):
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

    page_size = 1000

    raise_on_http_errors = True


    @property
    def url_base(self) -> str:
        return self.base_url

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        resp = response.json()
        has_more = resp.get("hasMore")
        if has_more:
            return {"offset": resp["offset"] + resp["count"]}
        return None

    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = {"limit": self.page_size}
        if next_page_token:
            params.update(**next_page_token)
        return params

    def request_headers(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
         return {"prefer": "transient", "Content-Type": "application/json"}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        records = response.json().get("items")
       
        if records:
            for record in records:
                # make sub-requests for each record fetched
                yield from self.fetch_record(record)

    def fetch_record(self, record: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        record_type = record.get("type").replace(" ", "").lower()
        url = self.base_url + RECORD_PATH + record_type + "/" + record.get("id")
        args = {"method": "GET", "url": url, "params": {"expandSubResources": True}}
        prep_req = self._session.prepare_request(requests.Request(**args))
        response = self._send_request(prep_req, request_kwargs={})
        # sometimes response.status_code == 400,
        # but contains json elements with error description,
        # to avoid passing it as {TYPE: RECORD}, we filter response by status
        if response.status_code == requests.codes.ok:
            record = response.json()
            record_with_type = {**record, "type": record_type}
            self.logger.info(f"Fetched record {record_with_type.get('id')} with datelastmodified {record_with_type.get(self.cursor_field)}")
            self.state = record_with_type
            yield record_with_type

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:
        current_cursor = stream_state.get(self.cursor_field, self.start_datetime)
        date_object_cursor = datetime.strptime(current_cursor, '%Y-%m-%dT%H:%M:%SZ')
        formated_cursor = date_object_cursor.strftime("%Y-%m-%d %H:%M:%S")
        return {
	        "q": "SELECT id, tranid, BUILTIN.DF(type) as type, to_char(lastModifiedDate, 'yyyy-mm-dd HH24:MI:SS') as lastmodified FROM transaction as t WHERE t.type = 'SalesOrd' AND to_char(lastModifiedDate, 'yyyy-mm-dd HH24:MI:SS') > '" + formated_cursor + "' ORDER BY lastModifiedDate ASC"
        }

# Basic incremental stream
class IncrementalNetsuite2Stream(Netsuite2Stream, IncrementalMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state = {}
        self._cursor_value = None
        
    state_checkpoint_interval = 100

    @property
    def cursor_field(self) -> str:
        return INCREMENTAL_CURSOR

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: str(self._cursor_value)}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        """
        Define state as a max between given value and current state
        """
        if not self._cursor_value:
            self._cursor_value = value[self.cursor_field]
        else:
            self._cursor_value = max(value[self.cursor_field], self.state[self.cursor_field])



class Transactions(IncrementalNetsuite2Stream):

    primary_key = "id"

    def path(self, **kwargs) -> str:
        return REST_PATH + "query/v1/suiteql"


# Source
class SourceNetsuite2(AbstractSource):


    logger: logging.Logger = logging.getLogger("airbyte")

    def auth(self, config: Mapping[str, Any]) -> OAuth1:
        # the `realm` param should be in format of: 12345_SB1
        realm = config["realm"].replace("-", "_").upper()
        return OAuth1(
            client_key=config["consumer_key"],
            client_secret=config["consumer_secret"],
            resource_owner_key=config["token_key"],
            resource_owner_secret=config["token_secret"],
            realm=realm,
            signature_method="HMAC-SHA256",
        )

    def base_url(self, config: Mapping[str, Any]) -> str:
        # the subdomain should be in format of: 12345-sb1
        subdomain = config["realm"].replace("_", "-").lower()
        return f"https://{subdomain}.suitetalk.api.netsuite.com"

    def get_session(self, auth: OAuth1) -> requests.Session:
        session = requests.Session()
        session.auth = auth
        return session
    
    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        auth = self.auth(config)
        object_types = config.get("object_types")
        base_url = self.base_url(config)
        session = self.get_session(auth)    
        # if `object_types` are not provided, use `Contact` object
        # there should be at least 1 contact available in every NetSuite account by default.
        url = base_url + RECORD_PATH + "contact"
        logger.info(f"Checking connection with {url}")
        try:
            response = session.get(url=url, params={"limit": 1})
            response.raise_for_status()
            return True, None
        except requests.exceptions.HTTPError as e:
            return False, e
  

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = self.auth(config)
        session = self.get_session(auth)
        base_url = self.base_url(config)

       
        
        input_args = {
            "auth": auth,
            "base_url": base_url,
            "start_datetime": config["start_datetime"],
            "window_in_days": config["window_in_days"],
        }
        return [Transactions(**input_args)]
