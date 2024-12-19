from entities.catalog_entity import CatalogLeaf
from entities.catalog_pages_index import CatalogPagesIndex
from entities.cursor import Cursor
from datetime import datetime
import asyncio
import requests
import jmespath
import logging
import json
from typing import List
from services.nuget_service import NugetService
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
     
    source_url = "https://api.nuget.org/v3/index.json"
    file_path = "./data/"
    scan_catalog_page_enable = False

    # initial catalog page scanning
    if scan_catalog_page_enable:
        catalog_indices = NugetService.getCatalogIndexUrl(source_url)
        source_url = catalog_indices[0]['@id']
        catalog_pages = NugetService.getCatalogPagesIndex(source_url) 
        # limited_pages = catalog_pages[:20]
        for page in catalog_pages:
            await NugetService.processPageItemsAync(page.id, file_path)

    # fetching binary data
    processed_page_id_file = "processed_pages.txt"
    pages_track_file = "pages_track.txt"
    unique_binaries_id_file = "unique_binaries_id.txt"
    unique_binaries_file = "unique_binaries_data.txt"

    with open(file_path+pages_track_file, "r") as file:
        pages_track_list = [line.strip() for line in file if line.strip()]
    

    # print(unique_binaries_id_set)
    for page in pages_track_list:
        with open(file_path+processed_page_id_file, "r") as file:
            processed_page_id_set = {line.strip() for line in file if line.strip()}
        
        # check if the page has already benn processed
        if page not in processed_page_id_set:
            res = await NugetService.asyncio_get(page)
            
            page_items = res.json()["items"]
            for item in page_items:
                item_id = item["nuget:id"]
                with open(file_path+unique_binaries_id_file, "r") as file:
                    unique_binaries_id_set = {line.strip() for line in file if line.strip()}
                # check if the item is already been processed
                if item_id not in unique_binaries_id_set:
                    item_url = item["@id"]
                    res = await NugetService.asyncio_get(item_url) 
                    delimiter = "#%"
                    await NugetService.write_to_file(file_path+unique_binaries_file, res.text+delimiter)
                    # mark the item to processed item
                    await NugetService.write_to_file(file_path+unique_binaries_id_file, item_id+"\n")

            # mark the page to proccessed page
            await NugetService.write_to_file(file_path+processed_page_id_file, page+"\n")

                    
if __name__ == "__main__":
    asyncio.run(main())