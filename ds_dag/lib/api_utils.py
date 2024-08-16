import asyncio
import json
from typing import List

import aiohttp
import requests
from aiohttp import ServerDisconnectedError
from requests import RequestException
from wix_legacy_logger.logger import WixLogger
from wix_trino_client.trino_connection import WixTrinoConnection

logger = WixLogger(__file__, level=WixLogger.INFO)


class ApiCallException(Exception):
    pass


class TrinoQueryExecutionException(Exception):
    pass


class JsonDecodeException(Exception):
    pass


def get_from_api(api_url: str, headers: dict = None, return_headers: bool = False):
    logger.info(f"Calling api: {api_url}")
    response = requests.get(api_url, headers=headers)
    if response.status_code != requests.codes.ok:
        raise ApiCallException(
            f"Api call 'get' returned unsuccessful with status {response.status_code}, url: {api_url}")
    try:
        if return_headers:
            return response.json(), response.headers
        else:
            return response.json()
    except json.JSONDecodeError:
        try:
            return response.text
        except Exception as e:
            raise JsonDecodeException(f"Failed to parse response from {api_url} with JSONDecodeError: {e}")


async def async_get_from_api(api_url: str):
    for attempt in range(1, 4):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url) as response:
                    return await response.text()
        except (RequestException, ServerDisconnectedError) as e:
            if attempt == 3:
                raise ApiCallException(f"Api call 'get' returned unsuccessful after {3} attempts: {e}")

            await asyncio.sleep(1)


def execute_trino_query(query: str, tc: WixTrinoConnection, add_cols_metadata: bool = False) -> List[tuple] | dict:
    try:
        return tc.execute_sql(query, add_cols_metadata=add_cols_metadata)
    except Exception as e:
        raise TrinoQueryExecutionException(f"Failed to execute query {query} with error: {e}")


def fail_on_request_error(r):
    if r.status_code != requests.codes.ok:
        print(f"Request to {r.url} has failed: {r.text}")
        r.raise_for_status()
    return r


def rename_table_from_to_table_to(table_from: str, table_to: str):
    execute_trino_query(f"ALTER TABLE {table_from} RENAME TO {table_to}")
    execute_trino_query(f"CREATE VIEW {table_from} AS SELECT * FROM {table_to}")
