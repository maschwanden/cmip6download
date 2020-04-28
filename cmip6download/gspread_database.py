import dataclasses
import datetime
import json
from pathlib import Path
import pprint
import time

import pandas as pd
import gspread
import gspread_pandas
from gspread_pandas import Spread, Client
from oauth2client.service_account import ServiceAccountCredentials

from cmip6download import core
from cmip6download import helper


creds_file = Path('/home/aschwanden/.config/cmip6download/gcreds.json')

col_names = \
    ['download_date'] + helper.METADATA_FILENAME_LIST + \
    ['filename', 'query_file']


class GoogleDataItemDatabase:
    COL_NAMES = col_names
    NCOLS = len(col_names)
    NROWS = int(1e2)
    SCOPE = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        ]
    SHARE_SHEETS_WITH = ['tristan.aschwanden@gmail.com']

    def __init__(self, credential_file, spreadsheet_name):
        print(credential_file, spreadsheet_name)
        self.credential_file = Path(credential_file)
        self.spreadsheet_name = spreadsheet_name

        self.config = gspread_pandas.conf.get_config(
            self.credential_file.parent, file_name=self.credential_file.name)
        self.creds = gspread_pandas.conf.get_creds(config=self.config)
        self.spread = self.get_spread()

    @classmethod
    def _share_spread(cls, spread):
        for x in cls.SHARE_SHEETS_WITH:
            if x in [x.get('emailAddress', None)
                     for x in spread.list_permissions()]:
                continue
            print(f'Share spreadsheet "{spread}" with {x}')
            spread.spread.share(x, perm_type='user', role='writer')

    def get_spread(self):
        spread = Spread(
            self.spreadsheet_name, creds=self.creds, create_spread=True)
        spread.delete_sheet('Sheet1') # Delete Default sheet called "Sheet1"
        self._share_spread(spread)
        return spread

    def get_sheet_name(self, name):
        return f'_{name}_'

    def open_sheet(self, name):
        name = self.get_sheet_name(name)
        try:
            self.spread.open_sheet(name)
        except gspread.WorksheetNotFound:
            self.spread.create_sheet(
                name, rows=self.NROWS, cols=self.NCOLS)
            self._init_sheet(name)

    def _init_sheet(self, name):
        print(f'Create init sheet "{name}"')
        self.spread.df_to_sheet(self._get_init_df(), index=False, sheet=name)

    def _get_init_df(self):
        df = pd.DataFrame(columns=self.COL_NAMES)
        return df

    def sheet_to_df(self, name):
        self.open_sheet(name)
        return self.spread.sheet_to_df(index=False)

    def df_to_sheet(self, name, df):
        self.open_sheet(name)
        try:
            self.spread.df_to_sheet(df, index=False)
        except gspread.exceptions.APIError:
            print('Probably the quota is exceeded! Wait for 2 min and try again...')
            time.sleep(120)
            self.spread.df_to_sheet(df, index=False)
        self.add_filter_to_sheet(name)

    def add_filter_to_sheet(self, name):
        self.open_sheet(name)
        self.spread.add_filter((0, 0))

    def _get_df_from_dataitems(self, data_items):
        data = {col: [] for col in self.COL_NAMES}
        for data_item in data_items:
            tmp = helper.get_metadata_from_filename(data_item.filename)
            tmp2 = dataclasses.asdict(data_item)
            for col in self.COL_NAMES:
                if col in tmp.keys():
                    data[col].append(tmp[col])
                elif col in tmp2.keys():
                    data[col].append(tmp2[col])
                else:
                    data[col].append(None)
        return pd.DataFrame(data, columns=self.COL_NAMES)

    def update_dataitems(self, data_items):
        df = self._get_df_from_dataitems(data_items)
        for name in df['variable_id'].unique():
            df_subset = df[df['variable_id'] == name]
            old_df = self.sheet_to_df(name)
            df_concat = pd.concat([old_df, df_subset])
            df_concat = df_concat.sort_values(
                'download_date', na_position='first')
            df_concat = df_concat.drop_duplicates(
                subset='filename', keep='last')
            df_concat = df_concat[list(self.COL_NAMES)]
            self.df_to_sheet(name, df_concat)
