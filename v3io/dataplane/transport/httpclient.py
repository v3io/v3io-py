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

import v3io.dataplane.request
import v3io.dataplane.response

from . import abstract


class Transport(abstract.Transport):
    _connection_timeout_seconds = 20
    _request_max_retries = 2

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None, verbosity=None):
        super(Transport, self).__init__(logger, endpoint, max_connections, timeout, verbosity)

        self._free_connections = queue.Queue()
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

    def close(self):
        with self._lock:
            # Avoid redundant calls to close
            if self._closed:
                return

            # Mark as closed before draining the queue to prevent race conditions
            self._closed = True

            connections = []
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
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot send request on a closed client")

        # Get a connection from the pool (thread-safe operation)
        try:
            connection = self._free_connections.get(block=True, timeout=30)
        except queue.Empty as e:
            raise RuntimeError("Timed out waiting for an available connection") from e

        try:
            return self._send_request_on_connection(request, connection)
        except BaseException as e:
            # Handle connection error in a thread-safe way
            with self._lock:
                if not self._closed:
                    with contextlib.suppress(Exception):
                        connection.close()
                    # Only create and add a new connection if we're not closed
                    try:
                        new_connection = self._create_connection(self._host, self._ssl_context)
                        self._free_connections.put(new_connection, block=False)
                    except Exception as conn_error:
                        self._logger.error(f"Failed to create replacement connection: {conn_error}")

            raise e

    def wait_response(self, request, raise_for_status=None, num_retries=1):
        connection = request.transport.connection_used
        is_retry = False

        while True:
            response_body = None
            status_code = None
            headers = None
            try:
                if is_retry:
                    with self._lock:
                        if self._closed:
                            raise RuntimeError("Transport closed during request retry")
                    request = self._send_request_on_connection(request, connection)
                    connection = request.transport.connection_used

                response = connection.getresponse()
                response_body = response.read()

                status_code, headers = self._get_status_and_headers(response)

                self.log("Rx", connection=connection, status_code=status_code, body=response_body)

                response = v3io.dataplane.response.Response(request.output, status_code, headers, response_body)

                # Return connection to pool if successful
                with self._lock:
                    if not self._closed:
                        try:
                            self._free_connections.put(connection, block=False)
                        except Exception as e:
                            self._logger.warn_with(
                                "Failed to return connection to pool", exception=str(e), connection_id=id(connection)
                            )
                            connection.close()
                response.raise_for_status(request.raise_for_status or raise_for_status)
                return response

            except v3io.dataplane.response.HttpResponseError as response_error:
                self._logger.warn_with(f"Response error: {response_error}")
                # Return connection to pool even on HTTP errors, as the connection is still valid
                with self._lock:
                    if not self._closed:
                        try:
                            self._free_connections.put(connection, block=False)
                        except Exception as e:
                            self._logger.warn_with(
                                "Failed to return connection to pool", exception=str(e), connection_id=id(connection)
                            )
                            connection.close()
                raise response_error
            except BaseException as e:
                # Handle connection error in thread-safe way
                with contextlib.suppress(Exception):
                    connection.close()
                with self._lock:
                    if self._closed:
                        raise RuntimeError("Transport closed during response handling") from e
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

                    # Return the new connection to the pool
                    with self._lock:
                        if not self._closed:
                            try:
                                self._free_connections.put(connection, block=False)
                            except Exception as e:
                                self._logger.warn_with(
                                    "Failed to return connection to pool",
                                    exception=str(e),
                                    connection_id=id(connection),
                                )
                                connection.close()

                    raise e

                self._logger.debug_with(
                    "Error occurred while waiting for response – retrying",
                    retries_left=num_retries,
                    e=type(e),
                    e_msg=e,
                )

            num_retries -= 1
            is_retry = True

    def _send_request_on_connection(self, request, connection):
        request.transport.connection_used = connection

        path = request.encode_path()

        self.log(
            "Tx", connection=connection, method=request.method, path=path, headers=request.headers, body=request.body
        )

        is_body_seekable = request.body and hasattr(request.body, "seek") and hasattr(request.body, "tell")
        starting_offset = request.body.tell() if is_body_seekable else 0
        retries_left = self._request_max_retries
        current_connection = connection  # Track the current connection for thread safety

        while True:
            try:
                # Check if transport is closed before proceeding
                with self._lock:
                    if self._closed:
                        raise RuntimeError("Transport closed during request sending")
                current_connection.request(request.method, path, request.body, request.headers)
                break
            except self._send_request_exceptions as e:
                self._logger.debug_with(
                    f"Disconnected while attempting to send request – "
                    f"{retries_left} out of {self._request_max_retries} retries left.",
                    e=type(e),
                    e_msg=e,
                )
                if retries_left == 0:
                    raise

                retries_left -= 1

                # Close failed connection
                with contextlib.suppress(Exception):
                    current_connection.close()
                # Create a new connection for retry, thread-safe
                with self._lock:
                    if self._closed:
                        raise RuntimeError("Transport closed during request retry") from e
                    current_connection = self._create_connection(self._host, self._ssl_context)

                if is_body_seekable:
                    # If the first connection fails, the pointer of the body might move at the size
                    # of the first connection blocksize.
                    # We need to reset the position of the pointer in order to send the whole file.
                    request.body.seek(starting_offset)

                # Update the connection in the request
                request.transport.connection_used = current_connection
            except BaseException as e:
                self._logger.error_with(
                    "Unhandled exception while sending request", e=type(e), e_msg=e, connection=current_connection
                )
                raise e

        # Update request with the potentially new connection
        request.transport.connection_used = current_connection
        return request

    def _create_connections(self, num_connections, host, ssl_context):
        with self._lock:
            for _ in range(num_connections):
                connection = self._create_connection(host, ssl_context)
                self._free_connections.put(connection, block=True)

    def _create_connection(self, host, ssl_context):
        if ssl_context is None:
            return http.client.HTTPConnection(host, timeout=self._connection_timeout_seconds)

        return http.client.HTTPSConnection(host, timeout=self._connection_timeout_seconds, context=ssl_context)

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
