"""
{{cookiecutter.name|title}} Adapter for Ingress
"""
import argparse
from datetime import datetime
from io import BytesIO
from typing import Optional, Dict, Tuple
import logging
import logging.config
from configparser import ConfigParser

from osiris.apis.ingress import Ingress
from osiris.core.azure_client_authorization import ClientAuthorization
from osiris.core.io import parse_date_str


logger = logging.getLogger(__file__)

DATE_FORMAT = '%Y%m%dT%H%M%SZ'


def main():
    """
    Reads configurations, sets up ingress-api, retrieves data and saves it to Ingress. If a time interval is given as
    an input argument, data is retrieved for this inteval. If not, it is based on the state file, which will also be
    updated after a successful run.
    """

    arg_parser = __init_argparse()
    args, _ = arg_parser.parse_known_args()

    config = ConfigParser()
    config.read(args.conf)
    credentials_config = ConfigParser()
    credentials_config.read(args.credentials)

    logging.config.fileConfig(fname=config['Logging']['configuration_file'],  # type: ignore
                              disable_existing_loggers=False)
    update_azure_logging(config)

    client_auth = ClientAuthorization(tenant_id=credentials_config['Authorization']['tenant_id'],
                                      client_id=credentials_config['Authorization']['client_id'],
                                      client_secret=credentials_config['Authorization']['client_secret'])

    ingress_api = Ingress(client_auth=client_auth,
                          ingress_url=config['Azure Storage']['ingress_url'],
                          dataset_guid=config['Datasets']['source'])

    max_interval_to_retrieve = pd.Timedelta(config.max_interval_to_retrieve)
    run_adapter_based_on_state_file = args.start_time is not None
    if run_adapter_based_on_state_file:
        start_time, end_time = extract_time_interval_from_state_file(state)
        next_start_time = retrieve_and_upload_data(start_time, end_time)

        state['next_start_time'] = datetime.strftime(next_start_time, DATE_FORMAT)
        state['last_successful_run'] = datetime.strftime(datetime.utcnow(), DATE_FORMAT)
        ingress_api.save_state(state)
    else:
        start_time, end_time = args.start_time, args.end_time
        retrieve_and_upload_data(start_time, end_time)


def retrieve_and_upload_data(start_time, end_time, max_interval_to_retrieve: timedelta):
    """
    Retrieves and uploads data to Ingress. If the input time interval is longer than max_interval_to_retrieve,
    it splits the interval and makes multiple retrievals and uploads.
    """

    if end_time - start_time <= max_interval_to_retrieve:
        retrieved_data, next_start_time = retrieve_data(start_time, end_time)
        upload_data_to_ingress(ingress_api, retrieved_data)
    else:
        retrieve_start_time = start_time
        retrieve_end_time = min(end_time, retrieve_start_time + max_interval_to_retrieve)

        while retrieve_start_time < end_time:
            retrieved_data, retrieve_start_time = retrieve_data(retrieve_start_time, retrieve_end_time)
            upload_data_to_ingress(ingress_api, retrieved_data)

            retrieve_end_time = min(end_time, retrieve_start_time + max_interval_to_retrieve)

            # TODO_TEMPLATE: Make sure that the loop breaks and logs error if the request is the same again and again.

        next_start_time = retrieve_start_time

    return next_start_time


def retrieve_data(start_time: datetime, end_time: datetime) -> Tuple([List(Dict(str))]):
    """
    Retrieves data from the source and stores it in a dataframe. The actual end time of the data is extracted and
    returned together with Ingress filename(s) and dataframe(s).
    """

    # TODO: Extract data and convert it to Pandas Dataframe. Example:
    dataframe = pd.read_csv('https://data.dk/csv_data', parse_dates=['timestamp'])

    # TODO: Extract actual data end time. Example:
    data_end_time = dataframe.timestamp.max() + timedelta(hours=1)
    filename = _get_filename(start_time, data_end_time, time_format='%Y%m%dT%H')

    # If you need to retrieve data from e.g. multiple endpoints, you can do something like:
    # data = []
    # data_end_times = []
    # for url in urls:
    #     dataframe = pd.read_csv(url)

    #     data_end_time = dataframe.timestamp.max() + timedelta(hours=1)
    #     filename = _get_filename(start_time, data_end_time, time_format='%Y%m%dT%H')

    #     data.append({filename, dataframe})

    # data_end_time = min(data_end_times)

    # return data, data_end_time

    return [{filename: dataframe}], data_end_time


def upload_data_to_ingress(ingress_api, retrieved_data):
    """
    Uploads data to Ingress.
    """
    for file_name, dataframe_to_ingest in retrieved_data.items():
        data_to_ingest = dataframe_to_ingest.to_json(orient='records', date_format='iso').encode('UTF-8')
        file = BytesIO(data_to_ingest)
        file.name = f'{file_name}.json'

        ingress_api.upload_file(file=file)
        logger.info(f'Data successfully uploaded: {file_name}')


def _get_filename(start_time, end_time, time_format='%Y%m%dT%H%M%SZ'):
    """Generates filename (without file extension) as time interval."""
    start_time_str = datetime.strftime(start_time, time_format)
    end_time_str = datetime.strftime(end_time, time_format)
    return f'{start_time_str}--{end_time_str}'


def extract_time_interval_from_state_file(state):
    """
    Generates start_time and end_time based on state file.
    """
    # TODO: Rewrite this so that it fits to your project
    start_time = datetime.strptime(state['next_start_time'], DATE_FORMAT)
    end_time = datetime.utcnow()

    return start_time, end_time


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
    parser.add_argument('--start_time',
                        nargs=1,
                        type=lambda s: parse_date_str(s),
                        help=f'setting the start time for the adapter.')
    parser.add_argument('--end_time',
                        nargs=1,
                        type=lambda s: parse_date_str(s),
                        default=datetime.utcnow(),
                        help=f'setting the end time for the adapter. If not given, it defaults to today.')

    return parser

def update_azure_logging(config):
    """Disable azure INFO logging from Azure"""
    if config.has_option('Logging', 'disable_logger_labels'):
        disable_logger_labels = config['Logging']['disable_logger_labels'].splitlines()
        for logger_label in disable_logger_labels:
            logging.getLogger(logger_label).setLevel(logging.WARNING)


if __name__ == "__main__":
    main()
