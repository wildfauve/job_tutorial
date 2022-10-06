from . import config

FAIL = 'fail'
OK = 'ok'

def exception_urn(instance):
    return "{urn_base}:error:{inst}".format(urn_base=config.JOB_URN_BASE, inst=instance)

class JobError(Exception):
    """
    Base Error Class for Job errors
    """

    def __init__(self, message="", name="", ctx={}, code=500, klass="", retryable=False, traceback: str = None):
        self.code = 500 if code is None else code
        self.retryable = retryable
        self.message = message
        self.name = name
        self.ctx = ctx
        self.klass = klass
        self.traceback = traceback
        super().__init__(self.message)

    def error(self):
        return {'error': self.message, 'code': self.code, 'step': self.name, 'ctx': self.ctx}


class SchemaMatchingError(JobError):
    pass