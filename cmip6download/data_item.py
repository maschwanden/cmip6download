import datetime
from pathlib import Path
from dataclasses import field, dataclass
import requests

from cmip6download import helper


logger = helper.get_logger(__file__)


HTTP_HEAD_TIMEOUT_TIME = 120
HTTP_DOWNLOAD_TIMEOUT_TIME = 1200
REQUESTS_CHUNK_SIZE = 128 * 1024


@dataclass
class BaseDataItem:
    filename: str
    local_base_dir: Path
    file_urls: list
    remote_checksum: str
    remote_checksum_type: str

    download_date: str = None
    download_successfull: bool = None
    _used_download_urls: list = None

    def __post_init__(self):
        self._file_url = None

    @property
    def local_file(self):
        return self.local_dir / self.filename

    @property
    def checksum_matches(self):
        return self.local_checksum == self.remote_checksum

    @property
    def local_checksum(self):
        if self.remote_checksum_type == 'SHA256':
            return helper.sha256_checksum_file(self.local_file)
        raise ValueError('Unkown checksum type!')

    @property
    def local_dir(self):
        return helper.get_local_dir(self.filename, self.local_base_dir)

    @property
    def file_url(self):
        if self._file_url is None:
            for fu in self.file_urls:
                try:
                    head = requests.head(
                        fu, allow_redirects=True, verify=False,
                        timeout=HTTP_HEAD_TIMEOUT_TIME)
                    if head.status_code == 200:
                        self._file_url = fu
                except:
                    pass
            if self._file_url is None:
                logger.debug(f'No file URL is available!')
                self._file_url = False
        return self._file_url

    def verify_download(self, verify_checksum=False):
        """Check if file was downloaded and optionally compare checksum."""
        verified = True
        if self.local_file.exists():
            if verify_checksum:
                if not self.checksum_matches:
                    verified = False
                    logger.debug(f'Local and remote checksum do no match.')
        else:
            logger.debug(f'File does not exist locally.')
            verified = False
        return verified

    def _http_get(self):
        """Make HTTP request for this data item and store it locally."""
        with requests.get(
                self.file_url, allow_redirects=True, verify=False,
                timeout=HTTP_DOWNLOAD_TIMEOUT_TIME, stream=True) as r:
            if r.status_code == 404:
                raise requests.HTTPError(404)
            try:
                with open(self.local_file, 'wb') as f:
                    for chunk in r.iter_content(REQUESTS_CHUNK_SIZE):
                        if not chunk:
                            break
                        f.write(chunk)
            except requests.exceptions.ChunkedEncodingError as e:
                logger.warning(
                    f'Could not finish download of {self.file_url} ({e})')

    def download(
            self, max_attempts=1, attempt=1, reverify_checksum=False,
            redownload=False):
        """
        High level download method that also checks if it is neccessary 
        to download the file again etc.
        """
        if self._used_download_urls is None:
            self._used_download_urls = []
        if redownload:
            pass
        elif self.verify_download(verify_checksum=reverify_checksum):
            return self.local_file
        if self.local_file.exists():
            self.local_file.unlink()
        self.local_dir.mkdir(exist_ok=True, parents=True)

        try:
            self._used_download_urls.append(self.file_url)
            if self.file_url:
                self._http_get()
        except (requests.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            logger.error(e)

        if self.verify_download(verify_checksum=True):
            logger.info(f'Download of {self.filename} successfull.')
            self.download_date = datetime.datetime.now()
            return self.local_file
        else:
            logger.info(f'Try to re-download... (attempt {attempt})')
            if attempt < max_attempts:
                if self.local_file.exists():
                    logger.warning(
                        f'Local file exists but is going to be deleted')
                    self.local_file.unlink()
                logger.warning(
                    'Download successfull but verification failed. '
                    f'Try to redownload ({attempt}th attempt).')
                return self.download(
                    max_attempts=max_attempts, attempt=attempt+1)
            logger.warning(f'Failed. Exceeded max number of attempts.')


@dataclass(unsafe_hash=True)
class CMIP6DataItem(BaseDataItem):
    cmip6_api_search_call: str = None
    query_file: str = None

    @property
    def metadata(self):
        return helper.get_metadata_from_filename(self.filename)

    @property
    def institution_filename_str(self):
        return f'[{self.metadata}] {self.filename}'
