import os
import aiohttp

import v3io.dataplane.request
import v3io.dataplane.response
import v3io.dataplane.transport


class Transport(object):

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None, verbosity=None):
        self._logger = logger
        self._endpoint = self._get_endpoint(endpoint)
        self._timeout = timeout
        self.max_connections = max_connections or 8
        # self._connector = None
        # self._client_session = None
        self._connector = aiohttp.TCPConnector()
        self._client_session = aiohttp.ClientSession(connector=self._connector)

        self._set_log_method(verbosity)

    async def close(self):
        await self._client_session.close()
        await self._connector.close()

    async def request(self,
                      container,
                      access_key,
                      raise_for_status,
                      encoder,
                      encoder_args,
                      output=None):

        # allocate a request
        request = v3io.dataplane.request.Request(container,
                                                 access_key,
                                                 raise_for_status,
                                                 encoder,
                                                 encoder_args,
                                                 output)

        path = request.encode_path()

        self.log('Tx', method=request.method, path=path, headers=request.headers, body=request.body)

        # call the encoder to get the response
        async with self._client_session.request(request.method,
                                                self._endpoint + '/' + path,
                                                headers=request.headers,
                                                data=request.body,
                                                ssl=False) as http_response:

            # get contents
            contents = await http_response.content.read()

            # create a response
            response = v3io.dataplane.response.Response(output,
                                                        http_response.status,
                                                        http_response.headers,
                                                        contents)

            # enforce raise for status
            response.raise_for_status(request.raise_for_status or raise_for_status)

            self.log('Rx', status_code=response.status_code, headers=response.headers, body=contents)

            return response

    @staticmethod
    def _get_endpoint(endpoint):

        if endpoint is None:
            endpoint = os.environ.get('V3IO_API')

            if endpoint is None:
                raise RuntimeError('Endpoints must be passed to context or specified in V3IO_API')

        if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
            endpoint = 'http://' + endpoint

        return endpoint.rstrip('/')

    def _set_log_method(self, verbosity):
        # by default, the log method is null
        log_method = self._log_null

        if verbosity == 'DEBUG':
            log_method = self._log

        setattr(self, 'log', log_method)

    def _log(self, message, *args, **kw_args):
        self._logger.debug_with(message, *args, **kw_args)

    def _log_null(self, message, *args, **kw_args):
        pass
