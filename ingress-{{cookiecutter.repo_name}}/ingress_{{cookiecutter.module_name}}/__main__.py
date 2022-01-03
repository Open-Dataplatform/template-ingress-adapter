"""
The main code to run the {{cookiecutter.name|title}} Ingress Adapter
"""
from datetime import datetime
import argparse
from configparser import ConfigParser
import logging
import logging.config

import pandas as pd
from osiris.apis.ingress import Ingress
from osiris.core.azure_client_authorization import ClientAuthorization
from osiris.core.io import parse_date_str

from . import adapter

logger = logging.getLogger(__file__)

DATE_FORMAT_ISO = '%Y%m%dT%H%M%SZ'


def __init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Ingress Adapter for {{cookiecutter.name|title}}')

    parser.add_argument('--conf',
                        nargs='+',
                        default=['conf.ini', '/etc/osiris/conf.ini'],
                        help='setting the configuration file')
    parser.add_argument('--credentials',
                        nargs='+',
                        default=['credentials.ini', '/vault/secrets/credentials.ini'],
                        help='setting the credential file')
    parser.add_argument('--from_date',
                        type=lambda s: parse_date_str(s)[0],
                        help='setting the start time for the adapter.')
    parser.add_argument('--to_date',
                        type=lambda s: parse_date_str(s)[0],
                        default=datetime.utcnow(),
                        help='setting the end time for the adapter. If not given, it defaults to today.')

    return parser


def update_azure_logging(config):
    """Disable azure INFO logging from Azure"""
    if config.has_option('Logging', 'disable_logger_labels'):
        disable_logger_labels = config['Logging']['disable_logger_labels'].splitlines()
        for logger_label in disable_logger_labels:
            logging.getLogger(logger_label).setLevel(logging.WARNING)


def initialize_ingress_api(config, credentials_config):
    """Returns Ingress API instance."""
    client_auth = ClientAuthorization(tenant_id=credentials_config['Authorization']['tenant_id'],
                                      client_id=credentials_config['Authorization']['client_id'],
                                      client_secret=credentials_config['Authorization']['client_secret'])

    ingress_api = Ingress(client_auth=client_auth,
                          ingress_url=config['Azure Storage']['ingress_url'],
                          dataset_guid=config['Datasets']['source'])

    return ingress_api


def main():
    """
    Reads configurations, sets up ingress-api, retrieves data and saves it to Ingress. If a time interval is given as
    an input argument, data is retrieved for this inteval. If not, it is based on the state file, which will also be
    updated after each upload.
    """

    arg_parser = __init_argparse()
    args, _ = arg_parser.parse_known_args()

    config = ConfigParser()
    config.read(args.conf)
    credentials_config = ConfigParser()
    credentials_config.read(args.credentials)

    logging.config.fileConfig(fname=config['Logging']['configuration_file'], disable_existing_loggers=False)
    update_azure_logging(config)

    ingress_api = initialize_ingress_api(config, credentials_config)
    max_interval_to_retrieve = pd.Timedelta(config['Datasets']['max_interval_to_retrieve'])
    date_format_in_state_file = config['Datasets']['date_format_in_state_file']

    # Get from and to dates
    run_adapter_based_on_state_file = args.from_date is not None
    if run_adapter_based_on_state_file:
        state = ingress_api.retrieve_state()
        from_date, to_date = adapter.extract_time_interval_from_state_file(state, date_format_in_state_file)
    else:
        from_date, to_date = args.from_date, args.retrieve_to_date

    # The following loop is used to split the time interval if it is longer than max_interval_to_retrieve. If it is
    # longer, multiple retrievals and uploads are made.
    retrieve_from_date = from_date
    while retrieve_from_date < to_date:
        retrieve_to_date = min(to_date, retrieve_from_date + max_interval_to_retrieve)

        # Retrieve and upload data
        retrieved_data, retrieve_from_date = adapter.retrieve_data(retrieve_from_date, retrieve_to_date)

        if retrieved_data is None:
            logger.info('No data retrieved in the interval from %s to %s.', retrieve_from_date, retrieve_to_date)
            break

        adapter.upload_data_to_ingress(ingress_api, retrieved_data)

        # TODO_TEMPLATE: Make sure that the loop breaks and logs error if the request is the same again and again.
        # Update state file
        if run_adapter_based_on_state_file:
            state['next_from_date'] = datetime.strftime(retrieve_from_date, date_format_in_state_file)
            state['last_successful_run'] = datetime.strftime(datetime.utcnow(), DATE_FORMAT_ISO)
            ingress_api.save_state(state)


if __name__ == "__main__":
    main()
