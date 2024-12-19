"""
This script scrapes NuGet's entire catalog in chronological order and saves the data to JSON files on local machine
Uses the process described here: https://learn.microsoft.com/en-us/nuget/guides/api/query-for-all-published-packages

Mainly intended for development purposes, allows creating a local/offline copy of the data that would be received
From querying NuGet to allow rapid testing without large amounts of network/bandwidth usage

Data is saved in the format:
    NuGet_Index.json                <-- Main index page which lists all catalog pages, contains paths to individual catalog pages
    NuGet_Catalog_Pages             <-- Directory containing all the retrieved catalog pages in JSON form
        NuGet_Catalog_Page#.json    <-- A single catalog page

    Within each file, is the raw data retrieved from that page's json response
    In the NuGet_Index, retrieved_data is a path to the catalog page file
        Ex: catalog_items["retrieved_data"] -> "NuGet_Catalog_Pages/NuGet_Catalog_Page0.json"

    Within each dict, a special key "retrieved_data" may be present which contains the result of an additional GET request for that data
    In each catalog file, retrieved_data directly contains a dictionary of the retrieved JSON data
        Ex: package_item["retrieved_data"] -> {"id":"package_name", "version": "1.3.8", ...}

Data is split in this way to reduce required memory usage, as saving all the data in a single large JSON file
Would require more RAM than most consumer computers have when loaded via json.load()
"""
from __future__ import annotations

import json
import requests
import time
import re
import signal
import dateutil.parser
import concurrent.futures

from pathlib import Path
from contextlib import suppress, contextmanager
from http import HTTPStatus
from argparse import ArgumentParser
from tqdm.auto import tqdm

from typing import Final
from typing import Any


@contextmanager
def block_signal(sig:signal.Signals|int):
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

def get_json(url:str, *, retries:int=5) -> dict[str, Any]:
    """Performs a GET request to the specified URL and returns the result of trying to read the response as JSON

    TODO: Improve error handling for different HTTP responses
    Currently just retries up to a max of @retries for any problem that occurs, then finally raises an exception if all fail

    :param url: URl to request data from
    :param retries: The number of times to retry the request if it fails
    :return: JSON-formatted data retrieved from the endpoint
    """
    for x in range(retries+1):
        try:
            web_request = requests.get(url)

            if web_request.status_code == HTTPStatus.TOO_MANY_REQUESTS:
                delay = web_request.headers.get('Retry-After', 1)
                time.sleep(delay)
                continue
            elif web_request.status_code != HTTPStatus.OK:
                #TODO: How do we want to handle other status codes? For now retry
                continue
            return web_request.json()

        except requests.exceptions.ConnectionError:
            time.sleep(1)
            continue

    #We were unable to scrape the data, raise an exception
    raise requests.exceptions.RequestException('Could not scrape, max retries exceeded')


NUGET_INDEX_URL:Final[str] = 'https://api.nuget.org/v3/index.json'
INDEX_FILE_NAME:Final[str] = 'NuGet_Index.json'
CATALOG_DIR_NAME:Final[str] = 'NuGet_Catalog_Pages'
PAGE_FILE_TEMPLATE:Final[str] = 'NuGet_Catalog_Page{page}.json'

#A special key which we store the data we retrieved from scraping under
DATA_KEY:Final[str] = 'retrieved_data'
def main():
    parser = ArgumentParser(
        description='Scrapes ALL NuGet catalog pages in JSON format',
    )
    parser.add_argument(
        '-o', '--output',
        required=False,
        type=Path, default=Path.cwd(),
        help='Output directory to place created files in. Defaults to current working directory',
    )
    parser.add_argument(
        '--skip_sub_check',
        default=False,
        action='store_true',
        help='Skip re-checking existing catalog JSON files. Will assume that if they exist, then they are complete',
    )
    args = parser.parse_args()

    args.output.mkdir(parents=False, exist_ok=True)
    json_index_file:Final[Path] = args.output.joinpath(INDEX_FILE_NAME)
    catalog_page_dir:Final[Path] = args.output.joinpath(CATALOG_DIR_NAME)
    page_regex = re.compile(r'page([0-9]+)\.json')

    #If the catalog index file already exists (perhaps stopped scraping during previous run)
    #Then retrieve and continue scraping from it rather than restarting from beginning
    if json_index_file.exists():
        with open(json_index_file, 'r') as f:
            catalog_data = json.load(f)
    else:
        #If the file doesn't exist, pull it from NuGet
        service_data = get_json(NUGET_INDEX_URL)
        resources = service_data['resources']
        catalog_entry = next(x for x in resources if 'Catalog' in x['@type'])
        catalog_api_url:Final[str] = catalog_entry['@id']

        catalog_data = get_json(catalog_api_url)

    #Sort the entries by the commit timestamp, which also happens to sort by increasing page number page 0,1,2,3...
    catalog_entries = catalog_data['items']
    catalog_entries.sort(key=lambda x: dateutil.parser.isoparse(x['commitTimeStamp']))

    #Show progress to user
    progress_iter = tqdm(
        catalog_entries,
        desc='Scraping Catalog Pages', colour='green',
        unit='Page',
        position=None, leave=None,
        disable=False,
    )
    for catalog_entry in progress_iter:
        page_number = int(page_regex.search(catalog_entry['@id']).group(1))
        if DATA_KEY in catalog_entry:
            page_file = Path(catalog_entry[DATA_KEY])
        else:
            page_file = catalog_page_dir.joinpath(PAGE_FILE_TEMPLATE.format(page=page_number))

        #Can be made a bit faster by skipping reading the page file if it exists, and assuming it's already done
        #However by default still reads and checks it in case any individual packages were missed/failed during last run
        if args.skip_sub_check and page_file.exists():
            continue

        #As with the main index, if the data already exists, load from disk instead of scraping new copy
        catalog_page_api_url = catalog_entry['@id']
        if page_file.exists():
            try:
                with open(page_file, 'r') as f:
                    catalog_page_data = json.load(f)
            except json.decoder.JSONDecodeError:
                #In case the JSON was improperly saved and cannot be read, re-download from web
                catalog_page_data = get_json(catalog_page_api_url)
        else:
            catalog_page_data = get_json(catalog_page_api_url)

        #Also sort the package entries by timestamp for consistent ordering
        package_entries = catalog_page_data['items']
        package_entries.sort(key=lambda x: dateutil.parser.isoparse(x['commitTimeStamp']))
        #Only process packages that we haven't already scraped and avoid re-downloading reduant data
        to_process = [x for x in package_entries if not x.get(DATA_KEY, None)]

        #Use multithreading to vastly improve performance, sending multiple requests at the same time improves throughput
        with concurrent.futures.ThreadPoolExecutor() as pool:
            threads = [
                pool.submit(get_json, package_entry['@id'])
                for package_entry in to_process
            ]

            #Show progress to user
            progress_bar = tqdm(
                concurrent.futures.as_completed(threads),
                total=len(threads),
                desc='Scraping Package Pages', colour='blue',
                unit='Package',
                position=None, leave=None,
                disable=not to_process,
            )
            list(progress_bar)

            for package_entry, future in zip(to_process, threads):
                with suppress(requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                    result = future.result()
                    package_entry[DATA_KEY] = result

        #Write out the data for the current page
        #Temporarily suppress SIGINT so that stopping the program with ctrl+c shouldn't corrupt the json data
        catalog_page_dir.mkdir(parents=False, exist_ok=True)
        with open(page_file, 'w') as f, block_signal(signal.SIGINT):
            json.dump(catalog_page_data, f, indent='\t')

        #Update and write the catalog data to reflect which file the corresponding data is contained in
        #Temporarily suppress SIGINT so that stopping the program with ctrl+c shouldn't corrupt the json data
        rel_path = page_file.relative_to(args.output)
        catalog_entry[DATA_KEY] = str(rel_path)
        with open(json_index_file, 'w') as f, block_signal(signal.SIGINT):
            json.dump(catalog_data, f, indent='\t')


if __name__ == '__main__':
    main()