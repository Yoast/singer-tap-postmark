"""Postmark cleaners."""
# -*- coding: utf-8 -*-

import base64
import logging
from datetime import datetime
from types import MappingProxyType
from typing import Any, Optional, Union

import arrow
from pandas import json_normalize


def clean_postmark_message_outbound(doc: dict) -> dict:
    """Clean postmark outbound messages."""
    # Remove useless data
    doc.pop('Attachments', None)
    doc.pop('Bcc', None)
    doc.pop('Cc', None)
    doc.pop('To', None)

    # Assume there is only 1 recipient and select it from the list
    if doc.get('Recipients'):
        # Convert to base64
        doc['Recipients'] = \
            base64.b64encode(doc['Recipients'][0].encode()).decode()

    # Convert to date
    doc['ReceivedAt'] = to_date(doc.get('ReceivedAt', ''))

    # Convert to none
    doc['TrackLinks'] = \
        False if doc.get('TrackLinks', 'None') == 'None' else doc['TrackLinks']
    doc['TrackOpens'] = \
        False if doc.get('TrackOpens', 'None') == 'None' else doc['TrackOpens']

    doc['_datetime'] = arrow.utcnow().datetime
    return doc


def clean_postmark_message_opens(docs: dict) -> dict:
    """Clean postmark opens."""
    # Normalize json
    df = json_normalize(docs, sep='_')

    # Remove columns
    df.drop(columns=['Geo_Coords', 'Geo_IP', 'Geo_Zip', 'Recipient'],
            inplace=True,
            errors='ignore')

    # Convert to date
    df['ReceivedAt'] = df.ReceivedAt.apply(to_date)

    data: dict = df.to_dict(orient='records')

    return data


def clean_postmark_stats_outbound_overview(doc: dict) -> dict:
    """Clean postmark bounces data."""
    # Insert date
    doc['_datetime'] = arrow.utcnow().datetime
    return doc


def clean_postmark_stats_outbound_bounces(doc: dict) -> dict:
    """Clean postmark bounces data."""
    # Create a total bounces field
    keys: list = list(doc.keys())
    keys.remove('Date')
    doc['Total'] = sum([doc[key] for key in keys])

    # Convert fields to date
    doc['Date'] = to_date(doc.get('Date', ''))

    # Insert date
    doc['_datetime'] = arrow.utcnow().datetime
    return doc


def clean_postmark_stats_outbound_platform(doc: dict) -> dict:
    """Clean postmark opens platforms data."""
    # Create a total opens field
    keys: list = list(doc.keys())
    keys.remove('Date')
    doc['Total'] = sum([doc[key] for key in keys])

    # Convert fields to date
    doc['Date'] = to_date(doc.get('Date', ''))

    # Insert date
    doc['_datetime'] = arrow.utcnow().datetime
    return doc


def clean_postmark_stats_outbound_clients(doc: dict) -> dict:
    """Clean postmark emailclients data."""
    # Convert fields to date
    doc['Date'] = to_date(doc.get('Date', ''))

    if doc.get('Email.cz'):
        doc['Email_cz'] = doc.pop('Email.cz')

    # Insert date
    doc['_datetime'] = arrow.utcnow().datetime
    return doc


def to_date(field: str) -> Union[datetime, str]:
    """Convert a field to date."""
    try:
        # If field exist and has value
        if field and len(field) > 0:
            return arrow.get(field).datetime
    # Handle exception
    except arrow.parser.ParserError as err:
        logging.error(err)
    return field


# Collect all cleaners
CLEANERS: MappingProxyType = MappingProxyType({
    'postmark_message_outbound': clean_postmark_message_outbound,
    'postmark_message_opens': clean_postmark_message_opens,
    'postmark_stats_outbound_overview': clean_postmark_stats_outbound_overview,
    'postmark_stats_outbound_bounces': clean_postmark_stats_outbound_bounces,
    'postmark_stats_outbound_platform': clean_postmark_stats_outbound_platform,
    'postmark_stats_outbound_clients': clean_postmark_stats_outbound_clients,
})
