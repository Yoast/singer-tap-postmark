"""PayPal API Client."""  # noqa: WPS226
# -*- coding: utf-8 -*-

import logging
from datetime import date, datetime
from types import MappingProxyType
from typing import Callable, Generator

import httpx
import singer
from dateutil.rrule import DAILY, rrule

from tap_postmark.cleaners import CLEANERS

# Example URL: https://api.postmarkapp.com/stats/outbound/opens/platforms

API_SCHEME: str = 'https://'
API_BASE_URL: str = 'api.postmarkapp.com'
API_MESSAGES_PATH: str = '/messages'
API_STATS_PATH: str = '/stats'
API_OUTBOUND_PATH: str = '/outbound'
API_OPENS_PATH: str = '/opens'
API_BOUNCE_PATH: str = '/bounces'
API_CLIENTS_PATH: str = '/emailclients'
API_PLATFORM_PATH: str = '/platforms'
API_DATE_PATH: str = '?fromdate=:date:&todate=:date:'

HEADERS: MappingProxyType = MappingProxyType({  # Frozen dictionary
    'Accept': 'application/json',
    'X-Postmark-Server-Token': ':token:',
})


class Postmark(object):  # noqa: WPS230
    """Postmark API Client."""

    def __init__(
        self,
        postmark_server_token: str,
    ) -> None:  # noqa: DAR101
        """Initialize client.

        Arguments:
            postmark_server_token {str} -- Postmark Server Token
        """
        self.postmark_server_token: str = postmark_server_token
        self.logger: logging.Logger = singer.get_logger()
        self.client: httpx.Client = httpx.Client(http2=True)

    def stats_outbound_bounces(  # noqa: WPS210, WPS432
        self,
        **kwargs: dict,
    ) -> Generator[dict, None, None]:  # noqa: DAR101
        """Get all bounce reasons from date.

        Raises:
            ValueError: When the parameter start_date is missing

        Yields:
            Generator[dict] --  Cleaned Bounce Data
        """
        # Validate the start_date value exists
        start_date_input: str = str(kwargs.get('start_date', ''))

        if not start_date_input:
            raise ValueError('The parameter start_date is required.')

        # Get the Cleaner
        cleaner: Callable = CLEANERS.get('postmark_stats_outbound_bounces', {})

        # Create Header with Auth Token
        self._create_headers()

        for date_day in self._start_days_till_now(start_date_input):

            # Replace placeholder in reports path
            from_to_date: str = API_DATE_PATH.replace(
                ':date:',
                date_day,
            )

            self.logger.info(
                f'Recieving Bounce stats from {date_day}'
            )

            # Build URL
            url: str = (
                f'{API_SCHEME}{API_BASE_URL}{API_STATS_PATH}'
                f'{API_OUTBOUND_PATH}{API_BOUNCE_PATH}{from_to_date}'
            )

            # Make the call to Postmark API
            response: httpx._models.Response = self.client.get(  # noqa: WPS437
                url,
                headers=self.headers,
            )

            # Raise error on 4xx and 5xxx
            response.raise_for_status()

            # Create dictionary from response
            response_data: dict = response.json()

            # Yield Cleaned results
            yield cleaner(date_day, response_data)

    def _start_days_till_now(self, start_date: str) -> Generator:
        """Yield YYYY/MM/DD for every day until now.

        Arguments:
            start_date {str} -- Start date e.g. 2020-01-01

        Yields:
            Generator -- Every day until now.
        """
        # Parse input date
        year: int = int(start_date.split('-')[0])
        month: int = int(start_date.split('-')[1].lstrip())
        day: int = int(start_date.split('-')[2].lstrip())

        # Setup start period
        period: date = date(year, month, day)

        # Setup itterator
        dates: rrule = rrule(
            freq=DAILY,
            dtstart=period,
            until=datetime.utcnow(),
        )

        # Yield dates in YYYY-MM-DD format
        yield from (date_day.strftime('%Y-%m-%d') for date_day in dates)

    def _create_headers(self) -> None:
        """Create authentication headers for requests."""
        headers: dict = dict(HEADERS)
        headers['X-Postmark-Server-Token'] = \
            headers['X-Postmark-Server-Token'].replace(
            ':token:',
            self.postmark_server_token,
        )
        self.headers = headers
