"""
{{cookiecutter.name|title}} Adapter for Ingress
"""

from typing import Optional

from osiris.core.configuration import ConfigurationWithCredentials
from osiris.adapters.ingress_adapter import IngressAdapter


configuration = ConfigurationWithCredentials(__file__)
config = configuration.get_config()
credentials_config = configuration.get_credentials_config()
logger = configuration.get_logger()


class {{cookiecutter.class_name}}Adapter(IngressAdapter):
    """
    The {{cookiecutter.name|title}} Adapter.
    Implements the retrieve_data method.
    """
    def __init__(self, ingress_url: str,  # pylint: disable=too-many-arguments
                 tenant_id: str,
                 client_id: str,
                 client_secret: str,
                 dataset_guid: str):
        super().__init__(ingress_url, tenant_id, client_id, client_secret, dataset_guid)

    def retrieve_data(self) -> Optional[bytes]:
        """
        Retrieves the data from {{cookiecutter.name|title}}.
        """
        logger.debug('Running the {{cookiecutter.name|title}} Ingress Adapter')

        # TODO: Implement code to retrieve data and return it as a bytes string.

        return b''

    @staticmethod
    def get_filename() -> str:

        # TODO: Implement a naming schema for filenames. This is the filename giving to the file containing
        # TODO: the data from retrieve_data in Azure storage. The name must be unique.

        # Here is an example for a naming schema for json data:
        # return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ') + '.json'

        return 'test.data'


def ingest_{{cookiecutter.module_name}}_data():
    """
    Setups the adapter and runs it.
    """
    adapter = {{cookiecutter.class_name}}Adapter(config['Azure Storage']['ingress_url'],
                                                      credentials_config['Authorization']['tenant_id'],
                                                      credentials_config['Authorization']['client_id'],
                                                      credentials_config['Authorization']['client_secret'],
                                                      config['Datasets']['source'])

    # TODO: use either:
    # TODO: adapter.upload_json_data(schema_validate: bool)
    # TODO: or
    # TODO: adapter.upload_data()
    # TODO: depending on your type of data.


if __name__ == "__main__":
    ingest_{{cookiecutter.module_name}}_data()
