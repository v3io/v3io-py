import requests

import v3io.dataplane.response
import v3io.dataplane.request
from . import abstract


class Transport(abstract.Transport):

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None):
        super(Transport, self).__init__(logger, endpoint, max_connections, timeout)
        self._next_connection_pool = 0
        self._session = requests.Session()

    def close(self):
        self._session.close()

    def send_request(self, request, transport_state=None):
        # call the encoder to get the response
        http_response = self._http_request(request.method,
                                           request.path,
                                           request.headers,
                                           request.body)

        # set http response
        setattr(request.transport, 'http_response', http_response)

        return request

    def wait_response(self, request, raise_for_status=None):

        # create a response
        response = v3io.dataplane.response.Response(request.output,
                                                    request.transport.http_response.status_code,
                                                    request.headers,
                                                    request.transport.http_response.content)

        # enforce raise for status
        response.raise_for_status(request.raise_for_status or raise_for_status)

        return response

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
