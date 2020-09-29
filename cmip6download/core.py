"""This script downloads CMIP6 data from the web.

In the configuration section a list search parameters can be specified
which is then used to search the CMIP6 website for data files and then
the user is asked if all these files should be downloaded. The files
are automatically sorted by their model, experiment, etc. and stored
in a accordingly named directory.

Required python packages:
- requests
- beautifulsoup4

Created March 2019
Last Updated October 2019
@author: Mathias Aschwanden (mathias.aschwanden@gmail.com)

"""
from dataclasses import field, dataclass
import datetime
import hashlib
import itertools
import logging
from pathlib import Path
from pprint import pformat, pprint
import re
import shutil
import sys
import urllib
import yaml

import requests
from bs4 import BeautifulSoup

import numpy as np

from cmip6download import helper
from cmip6download.config import CMIP6Config
from cmip6download import data_item


HTTP_HEAD_TIMEOUT_TIME = 120
HTTP_BASE_TIMEOUT_TIME = 240
HTTP_DOWNLOAD_TIMEOUT_TIME = 1200
REQUESTS_CHUNK_SIZE = 128 * 1024
