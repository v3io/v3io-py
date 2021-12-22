import os
import sys
import ujson

import v3io.dataplane.transport.requests
import v3io.dataplane.transport.httpclient
import v3io.dataplane.request
import v3io.dataplane.batch
import v3io.dataplane.response
import v3io.dataplane.output
import v3io.dataplane.kv_cursor
import v3io.aio.dataplane.transport.aiohttp
import v3io.common.helpers
import v3io.logger


class Client(object):

    def __init__(self,
                 logger=None,
                 endpoint=None,
                 access_key=None,
                 max_connections=None,
                 timeout=None,
                 logger_verbosity=None,
                 transport_verbosity='info',
                 retry_intervals = None):
        """Creates a v3io client, used to access v3io

        Parameters
        ----------
        logger (Optional) : logger
            An optional pre-existing logger. If not passed, a logger is created with 'logger_verbosity' level
        endpoint (Optional) : str
            The v3io endpoint to connect to (e.g. http://v3io-webapi:8081). if empty, the env var
            V3IO_API is used
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env. this can
            be overridden per request if needed
        max_connections (Optional) : int
            The number of connections to create towards v3io - defining the max number of parallel
            operations towards v3io. Defaults to 8
        timeout (Optional) : None
            For future use
        logger_verbosity (Optional) : INFO / DEBUG
            If 'logger' is not provided, this will specify the verbosity of the created logger.
        transport_verbosity (Optional) : INFO / DEBUG
            If set to 'DEBUG', transport will log lots of information at the cost of performance. It uses
            the "debug_with" logger interface, so wither a logger set to DEBUG level must be passed in 'logger' or
            'logger_verbosity' must be set to DEBUG
        retry_intervals (Optional) : tuple of float
            Tuple of intervals to use for exponential backoff in case of retries

        Return Value
        ----------
        A `Client` object
        """
        self._logger = logger or self._create_logger(logger_verbosity)
        self._access_key = access_key or os.environ.get('V3IO_ACCESS_KEY')

        if not self._access_key:
            raise ValueError('Access key must be provided in Client() arguments or in the '
                             'V3IO_ACCESS_KEY environment variable')

        self._transport = v3io.aio.dataplane.transport.aiohttp.Transport(self._logger,
                                                                         endpoint,
                                                                         max_connections,
                                                                         timeout,
                                                                         transport_verbosity,
                                                                         retry_intervals)

        # create models
        self.kv, self.object, self.stream, self.container = self._create_models()

    async def close(self):
        await self._transport.close()

    def _create_logger(self, logger_verbosity):
        logger = v3io.logger.Logger(level=logger_verbosity or 'INFO')
        logger.set_handler('stdout', sys.stdout, v3io.logger.HumanReadableFormatter())

        return logger

    @staticmethod
    def _get_schema_contents(key, fields):
        return ujson.dumps({
            'hashingBucketNum': 0,
            'key': key,
            'fields': fields
        })

    def _create_models(self):
        import v3io.aio.dataplane.object
        import v3io.aio.dataplane.kv
        import v3io.aio.dataplane.stream
        import v3io.aio.dataplane.container

        # create models
        return v3io.aio.dataplane.kv.Model(self), \
               v3io.aio.dataplane.object.Model(self), \
               v3io.aio.dataplane.stream.Model(self), \
               v3io.aio.dataplane.container.Model(self)
