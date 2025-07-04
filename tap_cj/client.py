"""GraphQL client handling, including CJStream base class."""

from __future__ import annotations

import decimal
import typing as t
from datetime import datetime, timedelta

import requests  # noqa: TC002
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import GraphQLStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

class DayChunkPaginator(BaseAPIPaginator):
    """A paginator that increments days in a date range."""

    def __init__(
        self,
        start_date: str,
        increment: int = 1,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(start_date)
        self._value = datetime.strptime(start_date, "%Y-%m-%d")
        self._end = datetime.today()
        self._increment = increment

    @property
    def end_date(self):
        """Get the end pagination value.

        Returns:
            End date.
        """
        return self._end

    @property
    def increment(self):
        """Get the paginator increment.

        Returns:
            Increment.
        """
        return self._increment
    
    def continue_if_empty(self, response: requests.Response) -> bool:
        return True

    def get_next(self, response: requests.Response):
        return (
            self.current_value + timedelta(days=self.increment)
            if self.has_more(response)
            else None
        )

    def has_more(self, response: requests.Response) -> bool:
        """Checks if there are more days to process.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return self.current_value < self.end_date


def set_none_or_cast(value: t.Any, expected_type: t.Type) -> t.Any:
    if value == "" or value is None:
        return None
    elif not isinstance(value, expected_type):
        return expected_type(value)
    else:
        return value


class CJStream(GraphQLStream):
    """CJ stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://commissions.api.cj.com/query"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["Authorization"] = "Bearer " + self.config.get("auth_token")
        return headers
    

    def get_new_paginator(self) -> DayChunkPaginator:
        return DayChunkPaginator(start_date=self.config.get("start_date"), increment=28)
    
    def get_url_params(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any] | str:
        params = {
            "PUB_ID": context.get("publisher_id"),
        }
        date_format_str = "%Y-%m-%d"
        next_page_date = datetime.strftime(next_page_token, date_format_str)
        if next_page_date:
            params["FROM_DATE"] = next_page_date
            end_datetime = datetime.strptime(
                next_page_date,
                date_format_str,
            ) + timedelta(days=28)
            params["TO_DATE"] = datetime.strftime(end_datetime, date_format_str)
        self.logger.info(f"params: {params}")
        return params

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare the data payload for the GraphQL API request.

        Developers generally should generally not need to override this method.
        Instead, developers set the payload by properly configuring the `query`
        attribute.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Dictionary with the body to use for the request.

        Raises:
            ValueError: If the `query` property is not set in the request body.
        """
        params = self.get_url_params(context, next_page_token)
        query = self.graphql_query

        if query is None:
            msg = "Graphql `query` property not set."
            raise ValueError(msg)

        if not query.lstrip().startswith("query"):
            # Wrap text in "query { }" if not already wrapped
            query = "query { " + query + " }"

        query = query.lstrip()
        query = (
            query.replace("$PUB_ID", params["PUB_ID"])
            .replace("$FROM_DATE", params["FROM_DATE"])
            .replace("$TO_DATE", params["TO_DATE"])
        )
        request_data = {
            "query": (" ".join([line.strip() for line in query.splitlines()])),
        }
        self.logger.debug("Attempting query:\n%s", query)
        return request_data


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        resp_json = response.json()
        if resp_json.get('data'):
            yield from resp_json.get("data", {}).get("publisherCommissions", {}).get(
                    "records",
                    [],
                )
        else:
            self.logger.error(f"Data is a None Object: {resp_json}")
            return []

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        for field_tuple in [
            ("orderDiscountUsd", float),
            ("pubCommissionAmountUsd", float),
            ("saleAmountUsd", float),
            ("totalCommissionPubCurrency", float),
            ("perItemSaleAmountPubCurrency", float),
            ("quantity", int),
        ]:
            field_name = field_tuple[0]
            field_type = field_tuple[1]
            if field_name in row:
                row[field_name] = set_none_or_cast(row[field_name], field_type)
            for i in range(len(row["items"])):
                if field_name in row["items"][i]:
                    row["items"][i][field_name] = set_none_or_cast(
                        row["items"][i][field_name],
                        field_type,
                    )
        return row
