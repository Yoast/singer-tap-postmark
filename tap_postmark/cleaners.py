"""Postmark cleaners."""
# -*- coding: utf-8 -*-

from types import MappingProxyType
from tap_postmark.streams import STREAMS
from typing import Any, Optional


class ConvertionError(ValueError):
    """Failed to convert value."""


def to_type_or_null(
    input_value: Any,
    data_type: Optional[Any] = None,
    nullable: bool = True,
) -> Optional[Any]:
    """Convert the input_value to the data_type.

    The input_value can be anything. This function attempts to convert the
    input_value to the data_type. The data_type can be a data type such as str,
    int or Decimal or it can be a function. If nullable is True, the value is
    converted to None in cases where the input_value == None. For example:
    a '' == None, {} == None and [] == None.

    Arguments:
        input_value {Any} -- Input value

    Keyword Arguments:
        data_type {Optional[Any]} -- Data type to convert to (default: {None})
        nullable {bool} -- Whether to convert empty to None (default: {True})

    Returns:
        Optional[Any] -- The converted value
    """
    # If the input_value is not equal to None and a data_type input exists
    if input_value and data_type:
        # Convert the input value to the data_type
        try:
            return data_type(input_value)
        except ValueError as err:
            raise ConvertionError(
                f'Could not convert {input_value} to {data_type}: {err}',
            )

    # If the input_value is equal to None and Nullable is True
    elif not input_value and nullable:
        # Convert '', {}, [] to None
        return None

    # If the input_value is equal to None, but nullable is False
    # Return the original value
    return input_value


def clean_row(row: dict, mapping: dict) -> dict:
    """Clean the row according to the mapping.

    The mapping is a dictionary with optional keys:
    - map: The name of the new key/column
    - type: A data type or function to apply to the value of the key
    - nullable: Whether to convert empty values, such as '', {} or [] to None

    Arguments:
        row {dict} -- Input row
        mapping {dict} -- Input mapping

    Returns:
        dict -- Cleaned row
    """
    cleaned: dict = {}

    key: str
    key_mapping: dict

    # For every key and value in the mapping
    for key, key_mapping in mapping.items():

        # Retrieve the new mapping or use the original
        new_mapping: str = key_mapping.get('map') or key

        # Convert the value
        cleaned[new_mapping] = to_type_or_null(
            row[key],
            key_mapping.get('type'),
            key_mapping.get('null', True),
        )

    return cleaned


def clean_postmark_stats_outbound_bounces(
    date_day: str,
    response_data: dict,
) -> dict:
    """Clean postmark bounces response_data.

    Arguments:
        response_data {dict} -- input response_data

    Returns:
        dict -- cleaned response_data
    """
    # Get the mapping from the STREAMS
    mapping: Optional[dict] = STREAMS['stats_outbound_bounces'].get(
        'mapping',
    )

    # Create Unique ID
    id = int(date_day.replace('-', ''))

    # Create new cleaned Dict
    cleaned_data: dict = {
        'id': id,
        'date': date_day,
        'AutoResponder': response_data.get('AutoResponder'),
        'Blocked': response_data.get('Blocked'),
        'DnsError': response_data.get('DnsError'),
        'HardBounce': response_data.get('HardBounce'),
        'SMTPApiError': response_data.get('SMTPApiError'),
        'SoftBounce': response_data.get('SoftBounce'),
        'SpamNotification': response_data.get('SpamNotification'),
        'Transient': response_data.get('Transient'),
        'Unknown': response_data.get('Unknown'),
    }
    response_data.pop('Days', None)
    return clean_row(cleaned_data, mapping)


def clean_postmark_stats_outbound_overview(
    date_day: str,
    response_data: dict,
) -> dict:
    """Clean postmark overview response_data.

    Arguments:
        response_data {dict} -- input response_data

    Returns:
        dict -- cleaned response_data
    """
    # Get the mapping from the STREAMS
    mapping: Optional[dict] = STREAMS['stats_outbound_overview'].get(
        'mapping',
    )
    # Create Unique ID
    id = int(date_day.replace('-', ''))

    response_data['id'] = id
    response_data['date'] = date_day
    return clean_row(response_data, mapping)


def clean_postmark_stats_outbound_platform(
    date_day: str,
    response_data: dict,
) -> dict:
    """Clean postmark platform opens response_data.

    Arguments:
        response_data {dict} -- input response_data

    Returns:
        dict -- cleaned response_data
    """
    # Get the mapping from the STREAMS
    mapping: Optional[dict] = STREAMS['stats_outbound_platform'].get(
        'mapping',
    )
    # Create Unique ID
    id = int(date_day.replace('-', ''))

    # Create new cleaned Dict
    # Days that didn’t produce statistics won’t appear in the JSON response.
    cleaned_data: dict = {
        'id': id,
        'date': date_day,
        'Desktop': response_data.get('Desktop'),
        'Mobile': response_data.get('Mobile'),
        'Unknown': response_data.get('Unknown'),
        'WebMail': response_data.get('WebMail'),
    }

    return clean_row(cleaned_data, mapping)


# Collect all cleaners
CLEANERS: MappingProxyType = MappingProxyType({
    'postmark_stats_outbound_bounces': clean_postmark_stats_outbound_bounces,
    'postmark_stats_outbound_overview': clean_postmark_stats_outbound_overview,
    'postmark_stats_outbound_platform': clean_postmark_stats_outbound_platform,
})
