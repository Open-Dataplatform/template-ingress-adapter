"""
{{cookiecutter.name|title}} Adapter for Ingress
"""
from datetime import datetime
from io import BytesIO
from typing import Optional, Dict, Tuple

from osiris.apis.ingress import Ingress
from osiris.core.configuration import ConfigurationWithCredentials
from osiris.core.azure_client_authorization import ClientAuthorization


configuration = ConfigurationWithCredentials(__file__)
config = configuration.get_config()
credentials_config = configuration.get_credentials_config()
logger = configuration.get_logger()


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


def main():
    """
    Setups the ingress-api, retrieves state, uploads data to ingress-api, saves state after successful upload.
    """

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

    # Get the next data to upload
    data_to_ingest, state = retrieve_data(state)

    # If no new data, return
    if data_to_ingest is None:
        logger.info('No new data to upload: {{cookiecutter.name|title}} Ingress Adapter')
        return

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
    # Option 4: Upload non-json formatted data to ingress time
    # - This case is not used often - no sample code below - example is ikontrol-adapter

    # TODO: Only keep the code from [START] to [END] of one of the options of the code below

    # [START] Option 1
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
    file = BytesIO(data_to_ingest)
    file.name = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ.json')
    # Set if schema validation is needed
    schema_validate = False

    # This call we raise Exception unless 201 is returned
    ingress_api.upload_json_file(file=file,
                                 schema_validate=schema_validate)
    # [END] Option 2

    # Save the state
    ingress_api.save_state(state)
    logger.info('Data successfully uploaded: {{cookiecutter.name|title}} Ingress Adapter')


if __name__ == "__main__":
    main()
