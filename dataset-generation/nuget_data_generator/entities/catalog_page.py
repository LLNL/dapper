class Catalog_Page():
    def __init__(self):
        self._id =""
        self._type=""
        self._commitTimeStamp=""
        self._count=""

    @property
    def id(self):
        return self._id
    
    @id.setter
    def id(self, id):
        self._id = id
    
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

    def __str__(self):
        return f"CatalogPage( id: {self.id}, type: {self.type}, commitTimeStamp: {self.commitTimeStamp}, count: {self.count})"