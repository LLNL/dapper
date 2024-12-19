"""
This scrapes all packages on PyPI and saves the data to JSON files on the local machine

Mainly intended for development purposes, allows creating a local/offline copy of the data that would be received
From querying PyPI to allow rapid testing without large amounts of network/bandwidth usage

Data is saved in the format:
    PyPI_Index.json                 <-- Main index page which lists all catalog pages, contains paths to individual catalog pages
    PyPI_PackagePages               <-- Directory containing all the retrieved catalog pages in JSON form
        <hash_prefix>               <-- A subdirectory based on hash values of the package name
            <hash>.json             <-- A single package page

    Within the PyPI_Index each package has an entry "retrieved_data" which is a path to find that package's specific json fole
        Ex: package_item["retrieved_data"] -> "PyPI_PackagePages/E7/PyPI_Package_<hash>.json"

The directories are laid out using the has prefix subdirectories due to systems (mainly Windows) running into issues with file-counts
It's not due to hitting a hard OS limit, but explorer tends to hang/take a long time indexing with that many files in a single directory
By splitting it into more subdirectories, each one doesn't have as many files in a single directory
So is less likely to hang when accessing them
"""
from __future__ import annotations

import json
import requests
import argparse
import time
import hashlib
import concurrent.futures
import signal
import math

from pathlib import Path
from http import HTTPStatus
from dataclasses import dataclass
from contextlib import suppress, contextmanager
from more_itertools import chunked
from tqdm.auto import tqdm

from typing import ClassVar, Final
from typing import Any


@contextmanager
def block_signal(sig):
    """During context, prevents the specified signal(s) from raising exceptions
    Restores the signal handling to the original state upon exiting the context

    Mainly used during write sections to prevent raised exceptions from resulting in incomplete/corrupt json files
    Which would cause errors on next run

    :param sig: THe signal(s) to suppress
    """
    original_handler = signal.getsignal(sig)
    signal.signal(sig, signal.SIG_IGN)
    yield
    signal.signal(sig, original_handler)

@dataclass
class PyPIPackage:
    package_name:str

    @property
    def hash(self) -> str:
        """Calculates the SHA-256 hash of the package's name
        Used to create unique filenames for the packages since some contain characters that are not-ideal to deal with

        :return: SHA-256 hash
        """
        sha256_hash = hashlib.sha256()
        sha256_hash.update(self.package_name.encode('utf-8'))
        return sha256_hash.hexdigest()

    @property
    def filepath(self) -> Path:
        """Computes the filepath to save the package's data based on the hash value of the package name

        Due to filesystems having issues with hundreds of thousands of files in a single directory
        Put files in subdirectories based on the first N characters of the filename hash

        :return: Relvative filepath to save data to
        """

        subdir = self.hash[:self._HASH_CHAR_COUNT]
        filename = self._FILENAME_TEMPLATE.format(hash=self.hash)
        return Path(subdir).joinpath(filename)

    def get_info(self, *, retries:int=5) -> dict[str, Any]:
        """Performs a GET request to the specified URL and returns the result of trying to read the response as JSON

        TODO: Improve error handling for different HTTP responses
        Currently just retries up to a max of @retries for any problem that occurs, then finally raises an exception if all fail

        :param retries: The number of times to retry the request if it fails
        :return: JSON-formatted data retrieved from the endpoint
        """
        url = self._API_URL.format(package_name=self.package_name)
        for x in range(retries+1):
            try:
                web_request = requests.get(url)

                match web_request.status_code:
                    case HTTPStatus.OK:
                        return web_request.json()
                    case HTTPStatus.TOO_MANY_REQUESTS:
                        delay = web_request.headers.get('Retry-After', 1)
                        time.sleep(delay)
                        continue
                    case HTTPStatus.NOT_FOUND:
                        break
                    case _:
                        #TODO: How do we want to handle other status codes? For now retry
                        continue

            except requests.exceptions.ConnectionError:
                time.sleep(1)
                continue

        #We were unable to scrape the data, raise an exception
        raise requests.exceptions.RequestException('Could not scrape, max retries exceeded')

    #============================== Class Attributes ==============================#
    _HASH_CHAR_COUNT:ClassVar[int] = 2
    _FILENAME_TEMPLATE:ClassVar[str] = 'PyPI_Package_{hash}.json'
    _API_URL:Final[str] = 'https://pypi.org/pypi/{package_name}/json'



PYPI_INDEX_URL:Final[str] = 'https://pypi.python.org/simple/'
INDEX_FILE_NAME:Final[str] = 'PyPI_Index.json'
PACKAGE_DIR_NAME:Final[str] = 'PyPI_PackagePages'

#A special key which we store the data we retrieved from scraping under
DATA_KEY:Final[str] = 'retrieved_data'
def main():
    parser = argparse.ArgumentParser(
        description='Scrapes ALL PyPI catalog pages in JSON format',
    )
    parser.add_argument(
        '-o', '--output',
        required=False,
        type=Path, default=Path.cwd(),
        help='Output directory to place created files in. Defaults to current working directory',
    )
    args = parser.parse_args()

    args.output.mkdir(parents=False, exist_ok=True)
    json_index_file:Final[Path] = args.output.joinpath(INDEX_FILE_NAME)
    json_page_dir:Final[Path] = args.output.joinpath(PACKAGE_DIR_NAME)

    #If the catalog index file already exists (perhaps stopped scraping during previous run)
    #Then retrieve and continue scraping from it rather than restarting from beginning
    if json_index_file.exists():
        with open(json_index_file) as f:
            catalog_info = json.load(f)
    else:
        #Ask it to send the response as JSON
        #If we don't set the "Accept" header this way, it will respond with HTML instead of JSON
        json_headers = {
            'Accept': 'application/vnd.pypi.simple.v1+json'
        }

        #TODO: Improve error handling/retry for web request instead of just failing
        web_request = requests.get(PYPI_INDEX_URL, headers=json_headers)
        if web_request.status_code != HTTPStatus.OK:
            raise requests.exceptions.HTTPError("Problem getting index")
        catalog_info = web_request.json()

    #Figure out which entries we still need to handle
    catalog_entries = catalog_info['projects']
    progress_iter = tqdm(
        catalog_entries,
        desc='Pre-checking package to scrape', colour='yellow',
        unit='Package',
        position=None, leave=None,
        disable=False,
    )
    to_process = [
        entry
        for entry in progress_iter
        if DATA_KEY not in entry
        or not args.output.joinpath(entry[DATA_KEY]).exists()
    ]

    #Break into chunks to process
    CHUNK_SIZE:Final[int] = 500
    chunked_entries = chunked(to_process, CHUNK_SIZE) #Process 500 at a time to speed up
    chunk_count = int(math.ceil(len(to_process) / CHUNK_SIZE))

    progress_iter = tqdm(
        chunked_entries,
        total=chunk_count,
        desc='Processing package slice', colour='green',
        unit='Slice',
        position=None, leave=None,
        disable=False,
    )
    for chunk in progress_iter:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            threads = [
                pool.submit(
                    PyPIPackage(package_entry['name']).get_info
                )
                for package_entry in chunk
            ]

            progress_bar = tqdm(
                concurrent.futures.as_completed(threads),
                total=len(threads),
                desc='Scraping Package', colour='blue',
                unit='Package',
                position=None, leave=None,
                disable=not to_process,
            )
            list(progress_bar)

            for package_entry, future in zip(chunk, threads):
                with suppress(requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                    package_info = future.result()

                    package = PyPIPackage(package_entry['name'])
                    package_file = json_page_dir.joinpath(package.filepath)

                    #Write the info to the package file
                    #Temporarily suppress SIGINT so that stopping the program with ctrl+c shouldn't corrupt the json data
                    json_page_dir.mkdir(parents=False, exist_ok=True)
                    package_file.parent.mkdir(parents=False, exist_ok=True) #Make sure the hash-prefix dir exists
                    with open(package_file, 'w', encoding='utf-8') as f, block_signal(signal.SIGINT):
                        json.dump(package_info, f, indent='\t', ensure_ascii=False)

                    rel_path = package_file.relative_to(args.output)
                    package_entry[DATA_KEY] = str(rel_path)

            #Update and write the main index to reflect which file the corresponding data is contained in
            #Temporarily suppress SIGINT so that stopping the program with ctrl+c shouldn't corrupt the json data
            with open(json_index_file, 'w', encoding='utf-8') as f, block_signal(signal.SIGINT):
                json.dump(catalog_info, f, indent='\t', ensure_ascii=False)


if __name__ == '__main__':
    main()