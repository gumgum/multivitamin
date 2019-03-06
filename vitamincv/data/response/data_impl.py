class Dictable():
    """Class that provides functionality to convert to dict"""
    @property
    def dict(self):
        return self.__dict__
    
    def __repr__(self):
        return self.__dict__
    
    def __str__(self):
        return str(self.__dict__)
