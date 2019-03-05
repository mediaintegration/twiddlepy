'''
    Base exception class for twiddle
'''
class twiddleException(Exception):
    def __init__(self, message, errors=None):
        super(twiddleException, self).__init__(message)



class SourceDataError(twiddleException):
    def __init__(self, message, errors=None):
        super(SourceDataError, self).__init__(message)
        self.errors = errors

class ExectionError(twiddleException):
    def __init__(self, message, errors=None):
        super(ExectionError, self).__init__(message)
        self.errors = errors

class LocationNotExist(twiddleException):
    def __init__(self, message, errors=None):
        super(LocationNotExist, self).__init__(message)
        self.errors = errors

class FieldExistsSolrSchema(twiddleException):
    def __init__(self, message, errors=None):
        super(FieldExistsSolrSchema, self).__init__(message)
        self.errors = errors

class FieldNotInSolrSchema(twiddleException):
    def __init__(self, message, errors=None):
        super(FieldNotInSolrSchema, self).__init__(message)
        self.errors = errors

class FieldTypeNotFound(twiddleException):
    def __init__(self, message, errors=None):
        super(FieldTypeNotFound, self).__init__(message)
        self.errors = errors

class MapperError(twiddleException):
    def __init__(self, message, errors=None):
        super(MapperError, self).__init__(message)
        self.errors = errors