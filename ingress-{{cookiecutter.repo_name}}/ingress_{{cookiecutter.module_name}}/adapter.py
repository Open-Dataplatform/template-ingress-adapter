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


logger = logging.getLogger(__file__)

def retrieve_data(state: Dict) -> Tuple[Optional[bytes], Dict]:
    """
    Retrieves the data from {{cookiecutter.name|title}}.
    """
    logger.info('Running the {{cookiecutter.name|title}} Ingress Adapter')

    # Given the state from last run, retrieve next batch of data and update state
    # - Notice, state will first be saved after successful upload

    # TODO: Implement code to retrieve data

    # TODO: Return data and state
    #       - If no new data in batch, return None and state (return None, state)

    # Example of how to return data
    # - if data is collected and prepared in a DataFrame: ingest_data_df
    # - then get the binary data: ingest_data_df.to_json(orient='records', date_format='iso').encode('UTF-8')
    # Example:
    # ingest_data = ingest_data_df.to_json(orient='records', date_format='iso').encode('UTF-8')
    # return ingest_data, state
    return b'', state

def retrieve_files(state: Dict) -> Tuple[Optional[Dict], Dict]:
    """
    Retrieves the data from {{cookiecutter.name|title}}.
    """
    logger.info('Running the {{cookiecutter.name|title}} Ingress Adapter')

    # Given the state from last run, retrieve next batch of files and update state
    # - Notice, state will first be saved after successful upload

    # TODO: Implement code to retrieve the files

    # TODO: Return files and state
    #       - If no new data in batch, return {} and state (return {}, state)

    # Example:
    # ingest_data = ingest_data_df.to_json(orient='records', date_format='iso').encode('UTF-8')
    # ingest_files = {"ingest_file_name":ingest_data}
    # return ingest_files, state
    return {}, state

def ingress_files(ingress_api, files_to_ingest):
    for file_name, data_to_ingest in files_to_ingest.items():
        file = BytesIO(data_to_ingest)
        file.name = file_name
        # This call we raise Exception unless 201 is returned
        ingress_api.upload_file(file=file)
        logger.info(f'Data successfully uploaded: {file_name}')

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

    return parser

def main():
    """
    Setups the ingress-api, retrieves state, uploads data to ingress-api, saves state after successful upload.
    """
    arg_parser = __init_argparse()
    args, _ = arg_parser.parse_known_args()

    config = ConfigParser()
    config.read(args.conf)
    credentials_config = ConfigParser()
    credentials_config.read(args.credentials)

    logging.config.fileConfig(fname=config['Logging']['configuration_file'],  # type: ignore
                              disable_existing_loggers=False)

    # To disable azure INFO logging from Azure
    if config.has_option('Logging', 'disable_logger_labels'):
        disable_logger_labels = config['Logging']['disable_logger_labels'].splitlines()
        for logger_label in disable_logger_labels:
            logging.getLogger(logger_label).setLevel(logging.WARNING)

    # Setup authorization
    client_auth = ClientAuthorization(tenant_id=credentials_config['Authorization']['tenant_id'],
                                      client_id=credentials_config['Authorization']['client_id'],
                                      client_secret=credentials_config['Authorization']['client_secret'])

    # Initialize the Ingress API
    ingress_api = Ingress(client_auth=client_auth,
                          ingress_url=config['Azure Storage']['ingress_url'],
                          dataset_guid=config['Datasets']['source'])

    # Get the state from last run.
    # - The state is kept in the ingress-guid as state.json, if it does not exist, an empty dict is returned
    state = ingress_api.retrieve_state()
    # There are 4 options to upload data, where the first 2 options cover most cases
    # Option 1: Upload to event time
    # - Use this option when
    #   - No transformation is needed afterward
    #   - No historic data is updated after ingestion to ingress
    # Option 2: Upload to ingress time
    # - Use this option when
    #   - A transformation is needed afterward
    #   - Historic data is updated after ingestion to ingress
    # Option 3: Upload non-json formatted data to event time
    # - This case is not used often - no sample code below
    # Option 3: Upload non-json formatted data to event time
    # - This case is not used often - no sample code below
    # Option 4: Upload non-json formatted files to ingress time
    # - Use this option when:
    #   - You wanna keep the raw files
    #   - One run can generate multiple files
    #   - A transformation is needed
    
    # TODO: Only keep the code from [START] to [END] of one of the options of the code below

    # [START] Option 1
    # Get the next data to upload
    data_to_ingest, state = retrieve_data(state)
    
    if data_to_ingest:
        file = BytesIO(data_to_ingest)
        file.name = 'data.json'
        # TODO: set event time (it should depend on the batch of data)
        # The time resolution is set by event_time, hence if the format follows
        # - If event_time = '' then data is stored in root folder
        # - If event_time = '2021' then data is stored on yearly basis
        # - If event_time = '2021-01' then data is stored on monthly basis
        # ...
        # - If event_time = '2021-01-01T01:01' then data is stored on minutely basis
        event_time = '2021-01-01T01:01'
        # Set if schema validation is needed
        schema_validate = False

        # This call we raise Exception unless 201 is returned
        ingress_api.upload_json_file_event_time(file=file,
                                                event_time=event_time,
                                                schema_validate=schema_validate)
    # [END] Option 1

    # [START] Option 2
    # Get the next data to upload
    data_to_ingest, state = retrieve_data(state)
    
    if data_to_ingest:
        file = BytesIO(data_to_ingest)
        file.name = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ.json')
        # Set if schema validation is needed
        schema_validate = False
    
        # This call we raise Exception unless 201 is returned
        ingress_api.upload_json_file(file=file,
                                     schema_validate=schema_validate)
    # [END] Option 2
    
    
    # [START] Option 4
    
    files_to_ingest, state = retrieve_files(state)
    ingress_files(ingress_api, files_to_ingest)
    
    # [END] Option 4
    
    # Save the state
    ingress_api.save_state(state)
    logger.info('Data successfully uploaded: {{cookiecutter.name|title}} Ingress Adapter')


if __name__ == "__main__":
    main()
