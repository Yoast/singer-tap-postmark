"""Postmark tap."""
# -*- coding: utf-8 -*-
import logging
from argparse import Namespace

import pkg_resources
from singer import get_logger, utils
from singer.catalog import Catalog

from tap_postmark.postmark import postmark
from tap_postmark.discover import discover
from tap_postmark.sync import sync

VERSION: str = pkg_resources.get_distribution('tap-postmark').version
LOGGER: logging.RootLogger = get_logger()
REQUIRED_CONFIG_KEYS: tuple = (
    'start_date',
    'report_user',
    'company_account',
    'user_password',
    'merchant_account',
)


@utils.handle_top_exception(LOGGER)
def main() -> None:
    """Run tap."""
    # Parse command line arguments
    args: Namespace = utils.parse_args(REQUIRED_CONFIG_KEYS)

    LOGGER.info(f'>>> Running tap-postmark v{VERSION}')

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog: Catalog = discover()
        catalog.dump()
        return

    # Otherwise run in sync mode
    if args.catalog:
        # Load command line catalog
        catalog = args.catalog
    else:
        # Loadt the  catalog
        catalog = discover()

    # Initialize postmark client
    postmark: postmark = postmark(
        args.config['postmark_server_token'],
    )

    sync(postmark, args.state, catalog, args.config['start_date'])


if __name__ == '__main__':
    main()
