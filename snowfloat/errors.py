class GeoError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'GeoError(message=%s)' % (self.message,)

class RequestError(GeoError):

    def __init__(self, status, code, message, more):
        self.status = status
        self.code = code
        self.message = message
        self.more = more

    def __str__(self):
        return 'RequestError(status=%d, code=%d, message=%s, more=%s)' % (
            self.status, self.code, self.message, self.more)
