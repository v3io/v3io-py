import os

import requests

import v3io.dataplane.response
import v3io.dataplane.request


class Transport(object):

    def __init__(self, logger, endpoint=None, max_connections=4, timeout=None):
        self._logger = logger
        self._endpoint = self._get_endpoint(endpoint)
        self._next_connection_pool = 0
        self._timeout = timeout
        self._session = requests.Session()

    def close(self):
        self._session.close()

    def request(self,
                container,
                access_key,
                raise_for_status,
                encoder,
                encoder_args,
                output=None,
                wait_response=True):

        # allocate a request
        request = v3io.dataplane.request.Request(container,
                                                 access_key,
                                                 raise_for_status,
                                                 encoder,
                                                 encoder_args,
                                                 output)

        if not wait_response:
            return request

        # send the request
        inflight_request = self.send_request(request)

        # wait for the response
        return self.wait_response(inflight_request)

    def send_request(self, request):

        # call the encoder to get the response
        http_response = self._http_request(request.method,
                                           request.path,
                                           request.headers,
                                           request.body)

        # set http response
        setattr(request.transport, 'http_response', http_response)

        return request

    def wait_response(self, request):

        # create a response
        return v3io.dataplane.response.Response(request.output,
                                                request.transport.http_response.status_code,
                                                request.headers,
                                                request.transport.http_response.text)

    def _http_request(self, method, path, headers=None, body=None):
        # self._logger.debug_with('Tx', method=method, path=path, headers=headers, body=body)

        response = self._session.request(method,
                                         self._endpoint + path,
                                         headers=headers,
                                         data=body,
                                         timeout=self._timeout,
                                         verify=False)

        # self._logger.debug_with('Rx', status_code=response.status_code, headers=response.headers, body=response.text)

        return response

    def _get_endpoint(self, endpoint):
        if endpoint is None:
            endpoint = os.environ.get('V3IO_API')

            if endpoint is None:
                raise RuntimeError('Endpoints must be passed to context or specified in V3IO_API')

        if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
            endpoint = 'http://' + endpoint

        return endpoint
