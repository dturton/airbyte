#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from collections import Counter
from json import JSONDecodeError

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from requests_oauthlib import OAuth1
from airbyte_cdk.sources.streams.http import HttpStream
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
        params = {"limit": 4} #TODO: make this configurable
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
        print ("fetch_record")
        yield record

    def request_body_json(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Optional[Mapping]:
        return {
	        "q": "SELECT id, tranid, type, to_char(lastModifiedDate, 'yyyy-mm-dd HH24:MI:SS') as lastmodified FROM transaction as t WHERE t.type = 'SalesOrd' AND to_char(lastModifiedDate, 'yyyy-mm-dd HH24:MI:SS') > '2023-02-08 16:02:42'" 
        }

# Basic incremental stream
class IncrementalNetsuite2Stream(Netsuite2Stream, ABC):
    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = 100

    @property
    def cursor_field(self) -> str:
        return INCREMENTAL_CURSOR

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


class Transactions(IncrementalNetsuite2Stream):
    # TODO: Fill in the cursor_field. Required.
    cursor_field = INCREMENTAL_CURSOR

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "id"

    def path(self, **kwargs) -> str:
        return REST_PATH + "query/v1/suiteql"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        return [None]


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
