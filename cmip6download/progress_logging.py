import dataclasses
from pathlib import Path

import numpy as np
import pandas as pd

from cmip6download import helper 


COL_NAMES = (
    ['download_date'] + helper.METADATA_FILENAME_LIST + 
    ['filename', 'query_file', 'cmip6_api_search_call'])


def log_download_progress(config, data_items):
    if not hasattr(config, 'progress_logging_directory'):
        raise AttributeError(
            'Configuration file must contain a '
            '"progress_logging_directory" entry')
    data_items_dict = {}
    for di in data_items:
        var = di.metadata['variable_id']
        try:
            data_items_dict[var].append(di)
        except KeyError:
            data_items_dict[var] = [di]

    for var, dis in data_items_dict.items():
        df_new = _get_df_from_dataitems(data_items)
        f = Path(config.progress_logging_directory).absolute() / f'{var}.csv'
        f.parent.mkdir(parents=True, exist_ok=True)
        try:
            df_old = pd.read_csv(f, header=0)
            df = pd.concat([df_old, df_new])
            df = df.sort_values(
                'download_date', na_position='first')
            df = df.drop_duplicates(
                subset='filename', keep='last')
        except FileNotFoundError:
            df = df_new
        df.to_csv(f, index=False)


def _get_df_from_dataitems(data_items):
    data = {col: [] for col in COL_NAMES}
    for data_item in data_items:
        metadata = helper.get_metadata_from_filename(data_item.filename)
        metadata2 = dataclasses.asdict(data_item)
        for col in COL_NAMES:
            if col in metadata.keys():
                data[col].append(metadata[col])
            elif col in metadata2.keys():
                data[col].append(metadata2[col])
            else:
                data[col].append(None)
    df = pd.DataFrame(data, columns=COL_NAMES)
    return df
