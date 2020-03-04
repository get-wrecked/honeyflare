class FileLockedError(Exception):
    '''The given file was already locked'''


class RetriableError(Exception):
    '''
    This is a temporary error that should cause the invocation to exit with an
    error so that it'll be retried.
    '''
