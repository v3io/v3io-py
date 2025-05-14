# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import contextlib
import http.client
import queue
import socket
import ssl
import threading
import time

import v3io.dataplane.request
import v3io.dataplane.response

from . import abstract

_connection_timeout_seconds = 20
_request_max_retries = 2


class Transport(abstract.Transport):

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None, verbosity=None):
        super(Transport, self).__init__(logger, endpoint, max_connections, timeout, verbosity)

        self._free_connections = queue.Queue(self.max_connections)
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._closed = False

        # based on scheme, create a host and context for _create_connection
        self._host, self._ssl_context = self._parse_endpoint(self._endpoint)

        # create the pool connection
        self._create_connections(self.max_connections, self._host, self._ssl_context)

        self._send_request_exceptions = (
            BrokenPipeError,
            http.client.CannotSendRequest,
            http.client.RemoteDisconnected,
            socket.timeout,
        )
        self._get_status_and_headers = self._get_status_and_headers_py3

    @classmethod
    def get_connection_timeout(cls):
        global _connection_timeout_seconds
        return _connection_timeout_seconds

    @classmethod
    def get_connection_acquire_timeout(cls):
        return cls.get_connection_timeout() * 10

    @classmethod
    def set_connection_timeout(cls, timeout):
        global _connection_timeout_seconds
        _connection_timeout_seconds = timeout

    @classmethod
    def set_request_max_retries(cls, retries):
        global _request_max_retries
        _request_max_retries = retries

    @classmethod
    def get_request_max_retries(cls):
        global _request_max_retries
        return _request_max_retries

    def _put_connection(self, connection):
        with self._lock:
            if self._closed:
                with contextlib.suppress(Exception):
                    connection.close()
                return
            try:
                self._free_connections.put(connection, block=False)
            except Exception as conn_error:
                self._logger.error(f"Failed to return connection to the pool: {conn_error}")
                with contextlib.suppress(Exception):
                    connection.close()
                raise conn_error

    def _get_connection(self):
        start_time = time.time()
        while True:
            # Check if we've exceeded the total timeout
            if time.time() - start_time > Transport.get_connection_acquire_timeout():
                raise TimeoutError(
                    f"Could not get a connection within {Transport.get_connection_acquire_timeout()} seconds"
                )
            # First, check state under lock and decide what to do
            with self._lock:
                if self._closed:
                    raise RuntimeError("Cannot send request on a closed client")

                # Try non-blocking get first
                if not self._free_connections.empty():
                    with contextlib.suppress(queue.Empty):
                        return self._free_connections.get_nowait()

            # Wait outside the lock
            try:
                connection = self._free_connections.get(block=True, timeout=0.01)
            except queue.Empty:
                continue  # Go back to the start of the loop
            except Exception as e:
                raise RuntimeError(f"Cannot get connection , {e}") from e

            # We got a connection, verify client is still open
            if self._closed:
                with contextlib.suppress(Exception):
                    connection.close()
                raise RuntimeError("Cannot send request on a closed client")
            return connection

    def close(self):
        with self._lock:
            if self._closed:
                return
            # Mark as closed before draining the queue to prevent race conditions
            self._closed = True
            connections = []
            # Move free connections to local variable to release the lock as soon as possible
            with contextlib.suppress(queue.Empty):
                while not self._free_connections.empty():
                    conn = self._free_connections.get_nowait()
                    connections.append(conn)

        self._logger.debug(f"Closing all {len(connections)} v3io transport connections")
        for conn in connections:
            try:
                conn.close()
            except Exception as e:
                self._logger.debug(f"Error closing connection: {e}")

    def requires_access_key(self):
        return True

    def send_request(self, request):
        connection = self._get_connection()
        try:
            return self._send_request_on_connection(request, connection)
        except BaseException:
            new_connection = self._create_connection(self._host, self._ssl_context)
            self._put_connection(new_connection)
            with contextlib.suppress(Exception):
                connection.close()

    def wait_response(self, request, raise_for_status=None, num_retries=1):
        connection = request.transport.connection_used
        is_retry = False

        while True:
            response_body = None
            status_code = None
            headers = None
            try:
                if is_retry:
                    request = self._send_request_on_connection(request, connection)
                    connection = request.transport.connection_used

                response = connection.getresponse()
                response_body = response.read()

                status_code, headers = self._get_status_and_headers(response)
                self.log(
                    "Rx",
                    connection=connection,
                    status_code=status_code,
                    body=response_body,
                )
                self._put_connection(connection)
                try:
                    v3io_response = v3io.dataplane.response.Response(
                        request.output, status_code, headers, response_body
                    )
                    v3io_response.raise_for_status(request.raise_for_status or raise_for_status)
                    return v3io_response
                except v3io.dataplane.response.HttpResponseError as response_error:
                    self._logger.warn_with(f"Response error: {response_error}")
                    raise response_error

            except BaseException as e:
                connection = self._create_connection(self._host, self._ssl_context)
                if num_retries == 0:
                    self._logger.error_with(
                        "Error occurred while waiting for response and ran out of retries",
                        e=type(e),
                        e_msg=e,
                        response_body=response_body,
                        status_code=status_code,
                        headers=headers,
                    )
                    self._put_connection(connection)
                    raise e

                self._logger.debug_with(
                    "Error occurred while waiting for response - retrying",
                    retries_left=num_retries,
                    e=type(e),
                    e_msg=e,
                )

            num_retries -= 1
            is_retry = True

    def _send_request_on_connection(self, request, connection):
        """Sends a request on the specified connection.

        This method attempts to send the given request over the provided connection.
        It handles potential connection errors, retries if necessary, and manages
        the request body's position for seekable streams. Note!! If the send operation fails,
        the connection is closed within this function.

        Args:
            request (Request): The request object to send.
            connection (http.client.HTTPConnection): The connection to use for sending.

        Returns:
            Request: The original request object.
        """

        path = request.encode_path()

        self.log(
            "Tx",
            connection=connection,
            method=request.method,
            path=path,
            headers=request.headers,
            body=request.body,
        )

        is_body_seekable = request.body and hasattr(request.body, "seek") and hasattr(request.body, "tell")
        starting_offset = request.body.tell() if is_body_seekable else 0
        retries_left = Transport.get_request_max_retries()

        while True:
            try:
                request.transport.connection_used = connection
                connection.request(request.method, path, request.body, request.headers)
                return request
            except self._send_request_exceptions as e:
                # Close failed connection
                with contextlib.suppress(Exception):
                    connection.close()

                self._logger.debug_with(
                    f"Disconnected while attempting to send request – "
                    f"{retries_left} out of {Transport.get_request_max_retries()} retries left.",
                    e=type(e),
                    e_msg=e,
                )
                if retries_left == 0:
                    raise

                retries_left -= 1

                connection = self._create_connection(self._host, self._ssl_context)

                if is_body_seekable:
                    # If the first connection fails, the pointer of the body might move at the size
                    # of the first connection blocksize.
                    # We need to reset the position of the pointer in order to send the whole file.
                    with contextlib.suppress(Exception):
                        request.body.seek(starting_offset)

            except BaseException as e:
                self._logger.error_with(
                    "Unhandled exception while sending request",
                    e=type(e),
                    e_msg=e,
                    connection=connection,
                )
                # Close failed connection
                with contextlib.suppress(Exception):
                    connection.close()
                raise e

        return request

    def _create_connections(self, num_connections, host, ssl_context):
        for _ in range(num_connections):
            connection = self._create_connection(host, ssl_context)
            self._put_connection(connection)

    def _create_connection(self, host, ssl_context):
        if ssl_context is None:
            return http.client.HTTPConnection(host, timeout=Transport.get_connection_timeout())

        return http.client.HTTPSConnection(host, timeout=Transport.get_connection_timeout(), context=ssl_context)

    def _parse_endpoint(self, endpoint):
        if endpoint.startswith("http://"):
            return endpoint[len("http://") :], None

        if endpoint.startswith("https://"):
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            return endpoint[len("https://") :], ssl_context

        return endpoint, None

    def _get_status_and_headers_py2(self, response):
        return response.status, response.getheaders()

    def _get_status_and_headers_py3(self, response):
        return response.code, response.headers
