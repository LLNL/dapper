import requests
from typing import List, Optional
from entities.catalog_entity import CatalogLeaf
from entities.catalog_pages_index import CatalogPagesIndex
from entities.catalog_page import Catalog_Page
from entities.page_items import PageItems
from typeguard import typechecked
import httpx
from concurrent.futures import ProcessPoolExecutor
import asyncio
import json
from dateutil.parser import parse
import aiofiles
import os

class NugetService:
    
    @staticmethod
    def getCursor():
        pass
    
    @typechecked
    @staticmethod
    def getCatalogIndexUrl(sourceUrl:str)->Optional[List[object]]:
        try:
            res = requests.get(sourceUrl).json()["resources"]
            return [item for item in res if item['@type'] == "Catalog/3.0.0"]
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

        return None
    

    @typechecked
    @staticmethod
    async def getCatalogIndexUrlAsync(sourceUrl:str)->Optional[List[object]]:
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(sourceUrl)
                catalog_index = res.json()["resources"]
                
                return [item for item in catalog_index if item["@type"] == "Catalog/3.0.0"]
        
        except httpx.RequestError as e:
            print(f"Request failed: {e}")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except httpx.TimeoutException:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}")

        return None
    
    @typechecked
    @staticmethod
    def getCatalogPagesIndex(sourceUrl)->Optional[List[object]]:
        try:
            res = requests.get(sourceUrl) 
            catalog_pages = NugetService.processCatalogPagesIndex(res.json())
            return catalog_pages
        except httpx.RequestError as e:
            print(f"Request failed: {e}")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except httpx.TimeoutException:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}") 
        
        return None
    
    @typechecked
    @staticmethod
    async def getCatalogPagesAsync(executor, sourceUrl)->Optional[List[object]]:
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(sourceUrl)
                # catalog_pages = await NugetService.asyncProcessCatalogPages(res.json(), executor)
                catalog_pages = NugetService.processCatalogPages(res.json())
                for page in catalog_pages:
                    print(page)
                return catalog_pages
        except httpx.RequestError as e:
            print(f"Request failed: {e}")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except httpx.TimeoutException:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}") 
        
        return None
    
    @typechecked
    @staticmethod
    def processCatalogPagesIndex(data:dict)->List:
        
        catalog_pages_index = CatalogPagesIndex()

        catalog_pages_index.id = data['@id']
        catalog_pages_index.type = data['@type']
        catalog_pages_index.commitTimeStamp = data['commitTimeStamp']
        catalog_pages_index.count = data['count']
        catalog_pages_index.lastCreated = data['nuget:lastCreated']
        catalog_pages_index.lastDeleted = data['nuget:lastDeleted']
        catalog_pages_index.lastEdited = data['nuget:lastEdited']
        
        for item in data['items']:
            catalog_page = Catalog_Page()
            catalog_page.id = item["@id"]
            catalog_page.type = item["@type"]
            catalog_page.commitTimeStamp = item["commitTimeStamp"]
            catalog_page.count = item["count"]
            catalog_pages_index.append(catalog_page)

        catalog_pages_index.context = data['@context']
        
        
        catalog_pages = catalog_pages_index.items
        

        return catalog_pages
    @typechecked 
    @staticmethod
    def getPageItems(page_items_url)->Optional[List]:
        try:
            res = requests.get(page_items_url) 
            catalog_items = NugetService.processCatalogPage(res.json())
            return catalog_items
        except httpx.RequestError as e:
            print(f"Request failed: {e}")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except httpx.TimeoutException:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}") 
        
        return None

    
    @typechecked
    @staticmethod
    def processCatalogPage(data:dict)->Optional[List]:
        page_items = PageItems()
        page_items.id = data["@id"]
        page_items.type = data["@type"]
        page_items.commitId = data["commitId"]
        page_items.commitTimeStamp = data["commitTimeStamp"]
        page_items.count = data["count"]
        page_items.parent = data["parent"]
        page_items.context = data["@context"]

        print(page_items)
    
        return None
    
    @typechecked 
    @staticmethod
    async def getPageItemsAsync(page_items_url, file_path):
        delimiter = "#%"
        track_path = file_path+"pages_track.txt"
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(page_items_url)
                id = res.json()["@id"]

                if not os.path.exists(track_path) or NugetService.check_pages(track_path, id) == False:
                    await NugetService.write_to_file(track_path, id+"\n")
                    data = res.text+delimiter
                    return data
                
        except httpx.RequestError as e:
            print(f"Request failed: {e}")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except httpx.TimeoutException:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}") 
        
        return None
    
    @staticmethod
    async def write_to_file(track_path, data):
        
        async with aiofiles.open(track_path, mode='a') as file:
            await file.write(data)

    @staticmethod
    async def processPageItemsAync(page_items_url, file_path):
        save_path = file_path+"pages.txt"
        

        try:
            print(f"Fetching data from {page_items_url}...")
            data = await NugetService.getPageItemsAsync(page_items_url, file_path)
            if data is not None:
                print(f"Write data to {file_path}...")
                await NugetService.write_to_file(save_path, data)
                print("Data written successfully.")
        except httpx.RequestError as e:
            print(f"An error occurred while fetching the API: {e}")

        except Exception as e:
            print(f"An error occurred: {e}")


    @staticmethod
    def check_pages(file_path, page_id):
        with open(file_path, "r") as file:
            id_set = {line.strip() for line in file if line.strip()}
        
        return page_id in id_set
    
    @staticmethod
    async def asyncio_get(item_url):
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(item_url)
                return res
            
               
        except httpx.RequestError as e:
            print(f"Request failed: {e}")
        except httpx.HTTPStatusError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except httpx.TimeoutException:
            print("Request timed out")
        except Exception as e:
            print(f"Unexpected error: {e}") 

        return None

    @staticmethod
    async def asyncProcessCatalogPages(data, executor: ProcessPoolExecutor):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, NugetService.processCatalogPages, data)
