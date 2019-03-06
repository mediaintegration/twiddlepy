'''
    Base exception class for twiddle
'''
class TwiddleException(Exception):
    def __init__(self, message, errors=None):
        super(TwiddleException, self).__init__(message)



class SourceDataError(TwiddleException):
    def __init__(self, message, errors=None):
        super(SourceDataError, self).__init__(message)
        self.errors = errors

class ExectionError(TwiddleException):
    def __init__(self, message, errors=None):
        super(ExectionError, self).__init__(message)
        self.errors = errors

class LocationNotExist(TwiddleException):
    def __init__(self, message, errors=None):
        super(LocationNotExist, self).__init__(message)
        self.errors = errors

class FieldExistsSolrSchema(TwiddleException):
    def __init__(self, message, errors=None):
        super(FieldExistsSolrSchema, self).__init__(message)
        self.errors = errors

class FieldNotInSolrSchema(TwiddleException):
    def __init__(self, message, errors=None):
        super(FieldNotInSolrSchema, self).__init__(message)
        self.errors = errors

class FieldTypeNotFound(TwiddleException):
    def __init__(self, message, errors=None):
        super(FieldTypeNotFound, self).__init__(message)
        self.errors = errors

class MapperError(TwiddleException):
    def __init__(self, message, errors=None):
        super(MapperError, self).__init__(message)
        self.errors = errors