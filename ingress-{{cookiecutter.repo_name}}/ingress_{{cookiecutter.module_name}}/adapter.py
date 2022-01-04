"""
Functions for the {{cookiecutter.name|title}} Ingress Adapter
"""
from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, Tuple, List
import logging

import pandas as pd

logger = logging.getLogger(__file__)


def retrieve_data(from_date: datetime, to_date: datetime) -> Tuple[List[Dict[str, pd.DataFrame]], datetime]:
    """
    Retrieves data from the source and stores it in a dataframe. The actual end time of the data is extracted and
    returned together with Ingress filename(s) and dataframe(s).
    """

    # TODO: Extract data and convert it to Pandas Dataframe. Example to emulate data retrieval:
    url = 'https://github.com/jiwidi/time-series-forecasting-with-python/blob/master/datasets/air_pollution.csv?raw=true'
    dataframe = pd.read_csv(url, parse_dates=['date'])
    dataframe = dataframe[(from_date <= dataframe['date']) & (dataframe['date'] <= to_date)]

    if dataframe.empty:
        return None, None

    # TODO: Extract actual data end time and generate filename. Example:
    data_end_date = dataframe.date.max() + timedelta(days=1)
    filename = _get_filename(from_date, data_end_date, time_format='%Y%m%d')

    # TODO_TEMPLATE: How to take care of empty dataframes or errors?

    # If you need to retrieve data from e.g. multiple endpoints, you can do something like:
    # data = []
    # data_end_dates = []
    # for url in urls:
    #     dataframe = pd.read_csv(url)

    #     data_end_date = dataframe.timestamp.max() + timedelta(hours=1)
    #     filename = _get_filename(from_date, data_end_date, time_format='%Y%m%dT%H')

    #     data.append({'filename': filename, 'data': dataframe})

    # data_end_date = min(data_end_dates)

    # return data, data_end_date

    return [{'filename': filename, 'data': dataframe}], data_end_date


def upload_data_to_ingress(ingress_api, retrieved_data):
    """
    Uploads data to Ingress.
    """
    for data_dict in retrieved_data:
        filename = data_dict['filename']
        dataframe_to_ingest = data_dict['data']

        file = BytesIO()
        file.name = f'{filename}.parquet'
        dataframe_to_ingest.to_parquet(file, engine='pyarrow', compression='snappy')
        file.seek(0)

        ingress_api.upload_file(file=file)
        logger.info("Data successfully uploaded: %s", filename)


def _get_filename(from_date, to_date, time_format='%Y%m%dT%H%M%SZ'):
    """Generates filename (without file extension) as time interval."""
    from_date_str = datetime.strftime(from_date, time_format)
    to_date_str = datetime.strftime(to_date, time_format)
    return f'{from_date_str}--{to_date_str}'


def extract_time_interval_from_state_file(state, date_format):
    """
    Generates from_date and to_date based on state file.
    """
    # TODO: Rewrite this so that it fits to your project
    from_date = datetime.strptime(state['next_from_date'], date_format)
    to_date = datetime.utcnow()

    return from_date, to_date
