from abc import ABC, abstractmethod
from datetime import datetime
from typing import List
from dateutil.parser import parse

class CatalogEntity(ABC):
    def __init__(self, url:str, commitTimeStamp:datetime, **kwargs):
        self._url = url
        self._commitTimeStamp = commitTimeStamp

    @property
    def url(self):
        return self._url
    
    @url.setter
    def url(self, url):
        self._url = url

    @property
    def commitTimeStamp(self):
        return self._commitTimeStamp
    
    @commitTimeStamp.setter
    def commitTimeStamp(self, commitTimeStamp):
        self._commitTimeStamp = commitTimeStamp
    
    @abstractmethod
    def items(self):
        pass

class CatalogLeaf():
    def __init__(self, **kwargs):
        # super().__init__(**kwargs)
        self._id = ""
        self._version = ""
        self._type =""
        self._comment=""
    
    @property
    def id(self):
        return self._id
    
    @id.setter
    def id(self, id:str):
        self._id = id
    
    @property
    def version(self):
        return self._version
    
    @version.setter
    def version(self, version:str):
        self._version = version
    
    @property
    def type(self):
        return self._type
    
    @type.setter
    def type(self, type:str):
        self._type = type

    @property
    def comment(self):
        return self._comment
    
    @comment.setter
    def comment(self, comment:str):
        self._comment = comment
    
    def __str__(self):
        return f"CatalogLeaf(id: {self.id}, type: {self.type}, comment: {self.comment})"
    


# class CatalogPage():
#     def __init__(self, **kwargs):
#         self._id=""
#         self._type=""
#         self._commitTimeStamp=""
#         self._count=0
    
    # @property
    # def id(self):
    #     return self._id
    
    # @id.setter
    # def id(self, id):
    #     self._id=id
    
    # @property
    # def type(self):
    #     return self._type
    
    # @type.setter
    # def type(self, type:str):
    #     self._type = type
    
    # @property
    # def commitTimeStamp(self):
    #     return self._commitTimeStamp
    
    # @commitTimeStamp.setter
    # def commitTimeStamp(self, time_stamp):
    #     self._commitTimeStamp = parse(time_stamp)
    
    # @property
    # def count(self):
    #     return self._count
    
    # @count.setter
    # def count(self, count):
    #     self._count = count
    # @property


    # def __str__(self):
    #     return f"CatalogPage( id: {self.id}, type: {self.type}, commitTimeStamp: {self.commitTimeStamp}, count: {self.count})"


# class CatalogIndex(CatalogEntity):
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self._items = []
    
#     @property
#     def items(self) -> List[CatalogPage]:
#         return self._items
    
#     @items.setter
#     def items(self, items:List):
#         self._items = items


