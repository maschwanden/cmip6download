from itertools import product
import hashlib
import logging
from pathlib import Path


LOGGER_LEVEL = logging.INFO


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
