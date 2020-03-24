import os
import ssl
import http.client

import v3io.dataplane.response
import v3io.dataplane.request


class Transport(object):

    def __init__(self, logger, endpoint=None, max_connections=4, timeout=None):
        self._logger = logger
        self._endpoint = self._get_endpoint(endpoint)
        self._timeout = timeout
        self._next_connection_idx = 0
        self.max_connections = max_connections

        # based on scheme, create a host and context for _create_connection
        self._host, self._ssl_context = self._parse_endpoint(self._endpoint)

        # create the pool connection
        self._connections = self._create_connections(max_connections,
                                                     self._host,
                                                     self._ssl_context)

    def close(self):
        for connection in self._connections:
            connection.close()

    def request(self,
                container,
                access_key,
                raise_for_status,
                transport_actions,
                encoder,
                encoder_args,
                output=None):

        # default to sending/receiving
        transport_actions = transport_actions or v3io.dataplane.transport.Actions.send_and_receive

        # allocate a request
        request = v3io.dataplane.request.Request(container,
                                                 access_key,
                                                 raise_for_status,
                                                 encoder,
                                                 encoder_args,
                                                 output)

        # if all we had to do is encode, return now
        if transport_actions == v3io.dataplane.transport.Actions.encode_only:
            return request

        # send the request
        inflight_request = self.send_request(request)

        # wait for the response
        return self.wait_response(inflight_request)

    def send_request(self, request, connection_idx=None):
        if connection_idx is None:
            connection_idx = self._get_next_connection_idx()

        # set the used connection on the request
        setattr(request.transport, 'connection_idx', connection_idx)

        # get a connection for the request and send it
        return self._send_request_on_connection(request, connection_idx)

    def wait_response(self, request, num_retries=1):
        connection_idx = request.transport.connection_idx
        connection = self._connections[connection_idx]

        while True:
            try:

                # read the response
                response = connection.getresponse()
                response_body = response.read()

                # create a response
                return v3io.dataplane.response.Response(request.output,
                                                        response.code,
                                                        response.headers,
                                                        response_body)

            except http.client.RemoteDisconnected as e:
                if num_retries == 0:
                    raise e

                num_retries -= 1

                # create a connection
                connection = self._recreate_connection_at_index(connection_idx)

                # re-send the request on the connection
                request = self._send_request_on_connection(request, connection_idx)

    def _send_request_on_connection(self, request, connection_idx):
        connection = self._connections[connection_idx]

        try:
            connection.request(request.method, request.path, request.body, request.headers)
        except (BrokenPipeError, http.client.CannotSendRequest, http.client.RemoteDisconnected):
            connection = self._recreate_connection_at_index(connection_idx)

            # re-request
            connection.request(request.method, request.path, request.body, request.headers)

        return request

    def _recreate_connection_at_index(self, connection_idx):

        # close the old connection
        self._connections[connection_idx].close()

        # create a new one and replace
        connection = self._create_connection(self._host, self._ssl_context)
        self._connections[connection_idx] = connection

        return connection

    def _create_connections(self, num_connections, host, ssl_context):
        connections = []

        for connection_idx in range(num_connections):
            connection = self._create_connection(host, ssl_context)
            connection.connect()
            connections.append(connection)

        return connections

    def _create_connection(self, host, ssl_context):
        if ssl_context is None:
            return http.client.HTTPConnection(host)

        return http.client.HTTPSConnection(host, context=ssl_context)

    def _get_endpoint(self, endpoint):
        if endpoint is None:
            endpoint = os.environ.get('V3IO_API')

            if endpoint is None:
                raise RuntimeError('Endpoints must be passed to context or specified in V3IO_API')

        if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
            endpoint = 'http://' + endpoint

        return endpoint

    def _parse_endpoint(self, endpoint):
        if endpoint.startswith('http://'):
            return endpoint[len('http://'):], None

        if endpoint.startswith('https://'):
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            return endpoint[len('https://'):], ssl_context

        return endpoint, None

    def _get_next_connection_idx(self):
        connection_idx = self._next_connection_idx

        self._next_connection_idx += 1
        if self._next_connection_idx >= len(self._connections):
            self._next_connection_idx = 0

        return connection_idx
