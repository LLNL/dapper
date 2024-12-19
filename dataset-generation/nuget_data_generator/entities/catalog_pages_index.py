from typing import List
from dateutil.parser import parse

class CatalogPagesIndex():
    def __init__(self, **kwargs):
        self._id=""
        self._type=""
        self._commitTimeStamp=""
        self._count=0
        self._lastCreated=""
        self._lastDeleted=""
        self._lastEdited=""
        self._items=[]
        self._context={}
    
    @property
    def id(self):
        return self._id
    
    @id.setter
    def id(self, id):
        self._id=id
    
    @property
    def type(self):
        return self._type
    
    @type.setter
    def type(self, type:str):
        self._type = type
    
    @property
    def commitTimeStamp(self):
        return self._commitTimeStamp
    
    @commitTimeStamp.setter
    def commitTimeStamp(self, time_stamp):
        self._commitTimeStamp = time_stamp
    
    @property
    def count(self):
        return self._count
    
    @count.setter
    def count(self, count):
        self._count = count
    @property
    def lastCreated(self):
        return self._lastCreated
    
    @lastCreated.setter
    def lastCreated(self, lastCreated):
        self._lastCreated=lastCreated

    @property
    def lastDeleted(self):
        return self._lastDeleted
    
    @lastDeleted.setter
    def lastDeleted(self, lastDeleted):
        self._lastDeleted=lastDeleted
    
    @property
    def lastEdited(self):
        return self._lastEdited
    
    @lastEdited.setter
    def lastEdited(self, lastEdited):
        self._lastEdited=lastEdited
    
    @property
    def items(self):
        return self._items
    
    @items.setter
    def items(self, items):
        self._items = items
    
    def append(self, item):
        self.items.append(item)
    
    @property
    def context(self):
        return self._context
    
    @context.setter
    def context(self, context):
        self._context=context

    def __str__(self):
        return f"CatalogPage( id: {self.id}, type: {self.type}, commitTimeStamp: {self.commitTimeStamp}, count: {self.count}, lastCreated: {self.lastCreated}, lastDeleted: {self.lastDeleted}, lastEdited: {self.lastEdited}, item_count: {len(self.items)}, context: {self.context})  "
