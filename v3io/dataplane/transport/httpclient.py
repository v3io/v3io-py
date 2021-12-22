import ssl
import sys
import http.client
import socket

import v3io.dataplane.response
import v3io.dataplane.request
from . import abstract
import queue

class Transport(abstract.Transport):

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None, verbosity=None):
        super(Transport, self).__init__(logger, endpoint, max_connections, timeout, verbosity)

        self._free_connections = queue.Queue()

        # based on scheme, create a host and context for _create_connection
        self._host, self._ssl_context = self._parse_endpoint(self._endpoint)

        # create the pool connection
        self._create_connections(self.max_connections,
                                                     self._host,
                                                     self._ssl_context)

        # python 2 and 3 have different exceptions
        if sys.version_info[0] >= 3:
            self._wait_response_exceptions = (
                http.client.RemoteDisconnected, ConnectionResetError, ConnectionRefusedError, http.client.ResponseNotReady)
            self._send_request_exceptions = (
                BrokenPipeError, http.client.CannotSendRequest, http.client.RemoteDisconnected)
            self._get_status_and_headers = self._get_status_and_headers_py3
        else:
            self._wait_response_exceptions = (http.client.BadStatusLine, socket.error)
            self._send_request_exceptions = (http.client.CannotSendRequest, http.client.BadStatusLine)
            self._get_status_and_headers = self._get_status_and_headers_py2

    def requires_access_key(self):
        return True
    
    def send_request(self, request):
        # TODO: consider getting param of whether we should block or not (wait for connection to be free or raise exception)
        connection = self._free_connections.get(block=True, timeout=None)

        # set the used connection on the request
        setattr(request.transport, 'connection_used', connection)

        # get a connection for the request and send it
        try:
            return self._send_request_on_connection(request, connection)
        except BaseException as e:
            self._free_connections.put(connection, block=True) 
            raise e


    def wait_response(self, request, raise_for_status=None, num_retries=1):
        connection = request.transport.connection_used

        while True:
            try:

                # read the response
                response = connection.getresponse()
                response_body = response.read()

                status_code, headers = self._get_status_and_headers(response)

                self.log('Rx',
                         connection=connection,
                         status_code=status_code,
                         body=response_body)

                response = v3io.dataplane.response.Response(request.output,
                                                            status_code,
                                                            headers,
                                                            response_body)

                # enforce raise for status
                response.raise_for_status(request.raise_for_status or raise_for_status)

                # return the response
                return response

            except self._wait_response_exceptions as e:
                if num_retries == 0:
                    self._logger.warn_with('Remote disconnected while waiting for response and ran out of retries',
                                           e=type(e),
                                           connection=connection)

                    raise e

                self._logger.debug_with('Remote disconnected while waiting for response',
                                        retries_left=num_retries,
                                        connection=connection)

                num_retries -= 1

                # make sure connections is closed (connection.connect is called automaticly when connection is closed)
                connection.close()

                # re-send the request on the connection
                request = self._send_request_on_connection(request, connection)
            except v3io.dataplane.response.HttpResponseError as response_error:
                self._logger.warn_with('Response error: {}'.format(str(response_error)))
                raise response_error
            except BaseException as e:
                self._logger.warn_with('Unhandled exception while waiting for response',
                                       e=type(e),
                                       connection=connection)
                raise e
            finally:
                self._free_connections.put(connection, block=True)

    def _send_request_on_connection(self, request, connection):
        path = request.encode_path()

        self.log('Tx',
                 connection=connection,
                 method=request.method,
                 path=path,
                 headers=request.headers,
                 body=request.body)

        try:
            connection.request(request.method, path, request.body, request.headers)
        except self._send_request_exceptions as e:
            self._logger.debug_with('Disconnected while attempting to send. Recreating connection', e=type(e))
            
            # re-request (connection.connect is called automaticly when connection is closed)
            connection.close()
            connection.request(request.method, path, request.body, request.headers)
        except BaseException as e:
            self._logger.warn_with('Unhandled exception while sending request', e=type(e))
            raise e

        return request
   
    def _create_connections(self, num_connections, host, ssl_context):
        for _ in range(num_connections):
            connection = self._create_connection(host, ssl_context)
            self._free_connections.put(connection, block=True)

    def _create_connection(self, host, ssl_context):
        if ssl_context is None:
            return http.client.HTTPConnection(host)

        return http.client.HTTPSConnection(host, context=ssl_context)

    def _parse_endpoint(self, endpoint):
        if endpoint.startswith('http://'):
            return endpoint[len('http://'):], None

        if endpoint.startswith('https://'):
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            return endpoint[len('https://'):], ssl_context

        return endpoint, None

    def _get_status_and_headers_py2(self, response):
        return response.status, response.getheaders()

    def _get_status_and_headers_py3(self, response):
        return response.code, response.headers
