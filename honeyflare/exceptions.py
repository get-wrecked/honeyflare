class RetriableError(Exception):
    """
    This is a temporary error that should cause the invocation to exit with an
    error so that it'll be retried.
    """


class FileLockedError(RetriableError):
    """The given file was already locked"""


class ExportFailedError(RetriableError):
    """A batch of spans could not be sent to the collector"""
