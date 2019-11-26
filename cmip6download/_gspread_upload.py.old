from pathlib import Path
import pprint

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from cmip6download import CONFIG_DIR, CONFIG


def get_sheet():
    cred_file = Path(CONFIG.google_credentials_file)
    if not cred_file.is_absolute():
        cred_file = CONFIG_DIR / cred_file
    sheet_path = CONFIG.google_sheet_path
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)
    client = gspread.authorize(creds)
    return client.open(sheet_path)


def group_data_items_by_variable(data_items):
    groups = {}
    for item in data_items:
        metadata = item.get_metadata_from_filename()
        print(metadata)
        if metadata['variable_id'] not in groups.keys():
            groups[metadata['variable_id']] = []
        groups[metadata['variable_id']].append(item)
    return groups


def get_update_status_data(data_item):
    print('update_download_status', data_item)
    data = data_item.get_metadata_from_filename()
    print(data)


def update_status(data_items):
    sheet = get_sheet()
    data_item_groups = group_data_items_by_variable(data_items)
    for variable, group in data_item_groups.items():
        print(variable, group)
    return
    for data_item in data_items:
        data = data_item.get_metadata_from_filename()

        print(data)

#
# pp = pprint.PrettyPrinter()
# pp.pprint(sheet.get_all_records())
