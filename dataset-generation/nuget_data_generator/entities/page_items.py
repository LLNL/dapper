class PageItems:
    def __init__(self):
        self._id=""
        self._type=""
        self._commitId=""
        self._commitTimeStamp=""
        self._count=0
        self._parent=""
        self._items=[]
        self._context={}

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
    def type(self, type):
        self._type = type

    @property
    def commitId(self):
        return self._commitId
    
    @commitId.setter
    def commitId(self, commit_id):
        self._commitId=commit_id

    @property
    def commitTimeStamp(self):
        return self._commitTimeStamp
    
    @commitTimeStamp.setter
    def commitTimeStamp(self, commit_time_stamp):
        self._commitTimeStamp = commit_time_stamp
    
    @property
    def count(self):
        return self._count
    
    @count.setter
    def count(self, count):
        self._count =count

    @property
    def parent(self):
        return self._parent
    
    @parent.setter
    def parent(self, parent):
        self._parent = parent

    @property
    def items(self):
        return self.items
    
    @items.setter
    def items(self, items):
        self._items = items
    
    def append(self, items):
        self._items.append(items)

    @property
    def context(self):
        return self._context
    
    @context.setter
    def context(self, context):
        self._context = context
    
    def __str__(self):
        return f"PageItems( id: {self.id}, type: {self.type}, commitId: {self.commitId}, commitTypeStamp: {self.commitTimeStamp}, count: {self.count}, parent: {self.parent}, item_count: {len(self.items)}, context: {self.context} )"