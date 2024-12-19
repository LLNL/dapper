from datetime import datetime

class Cursor():

    def __init__(self):
        self._datetime = None
    
    @property
    def datetime(self):
        return self._datetime
    
    @datetime.setter
    def datetime(self, datetime:datetime):
        self._datetime = datetime