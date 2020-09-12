from itertools import product
import hashlib
import logging
from pathlib import Path


LOGGER_LEVEL = logging.INFO

METADATA_FILENAME_LIST = [
    'variable_id',
    'table_id',
    'source_id',
    'experiment_id',
    'member_id',
    'grid_label',
    'time_range',
    ]


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.setLevel(LOGGER_LEVEL)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOGGER_LEVEL)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    file_handler = logging.FileHandler('error.log')
    file_handler.setLevel(LOGGER_LEVEL)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def ask_user(question):
    print(question)
    yesno = ''
    while yesno not in ['yes', 'no', 'y', 'n']:
        yesno = input('[y]es/[n]o > ')
    if yesno == 'no' or yesno == 'n':
        return False
    return True


def sha256_checksum_file(path, block_size=65536):
    if not path.exists():
        raise FileNotFoundError()
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(block_size), b''):
            sha256.update(block)
    res = sha256.hexdigest()
    return res


def get_all_nc_files_in_directory(base_directory):
    p = Path(base_directory)
    files = []
    for f in p.glob('**/*'):
        if f.is_file() and f.name.endswith('.nc'):
            files.append(f)
    return files


def remove_tmp_files(base_directory):
    p = Path(base_directory)
    files = []
    for f in p.glob('**/*'):
        if f.is_file() and f.name.endswith('.tmp'):
            files.append(f)
    if not files:
        print('No tmp files found.')
        return

    print('tmp files:')
    for f in files:
        print(f)
    if ask_user('Remove these tmp files now?'):
        for f in files:
            f.unlink()
            print(f'Deleted {f}.')


def dict_product(d):
    keys = d.keys()
    for element in product(*d.values()):
        yield dict(zip(keys, element))


def get_local_dir(filename, local_data_dir):
    metadata = get_metadata_from_filename(filename)
    try:
        subdir_names = [
            metadata['variable_id'], metadata['table_id'],
            metadata['experiment_id'], metadata['source_id'],
            metadata['member_id'], metadata['grid_label'],
            ]
        return Path(local_data_dir).joinpath(*subdir_names)
    except KeyError:
        logger.debug(f'Could not retrieve local dir from given filename.')


def get_metadata_from_filename(filename):
    metadata = dict(
        zip(METADATA_FILENAME_LIST,
        filename.split('.')[0].split('_'),
        )
    )
    metadata['filename'] = filename
    return metadata


def move_files_to_local_dir(files, local_data_dir):
    for old_f in files:
        if not old_f:
            continue
        old_f = Path(old_f)
        local_dir = get_local_dir(old_f.name, local_data_dir)
        if not local_dir:
            continue
        local_dir.mkdir(exist_ok=True, parents=True)
        new_f = Path(local_dir / str(old_f.name))
        if old_f != new_f:
            logger.info(f'Move {old_f} to {new_f}.')
            try:
                shutil.move(str(old_f), str(new_f))
            except FileNotFoundError:
                logger.warning(f'FileNotFoundError: {old_f}')
