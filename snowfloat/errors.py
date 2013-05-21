"""Errors definitions."""

class Error(Exception):
    """Top error class. All errors should derive this class.
    
    Attributes:
        message (str): Error message.
    """

    def __init__(self, message):
        Exception.__init__(self, message)

    def __str__(self):
        return 'Error(message=%s)' % (self.message,)

class RequestError(Error):
    """Server request error.
    
    Attributes:
        status (int): Error http status code.
        code (int): Internal error code.
        more (str): Additional details.
    """

    status = None
    code = None
    more = None

    def __init__(self, status, code, message, more):
        Error.__init__(self, message)
        self.status = status
        self.code = code
        self.more = more

    def __str__(self):
        return 'RequestError(status=%s, code=%s, message=%s, more=%s)' % (
            self.status, self.code, self.message, self.more)
