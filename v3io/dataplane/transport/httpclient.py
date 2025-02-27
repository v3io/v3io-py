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
import gc
import http.client
import json
import queue
import socket
import ssl
import sys
import threading
import time
import traceback

import v3io.dataplane.request
import v3io.dataplane.response

from . import abstract


def get_connection_pool_stats(transport):
    """Collect comprehensive statistics about the connection pool state."""
    pool_stats = {
        "timestamp": time.time(),
        "free_connections_size": transport._free_connections.qsize() if transport._free_connections else 0,
        "free_connections_empty": transport._free_connections.empty() if transport._free_connections else True,
        "max_connections": transport.max_connections,
        "active_thread_count": threading.active_count(),
        "ssl_version": ssl.OPENSSL_VERSION,
        "python_version": sys.version,
    }

    # Get details about all HTTPConnection objects
    connection_objects = []
    for obj in gc.get_objects():
        if isinstance(obj, http.client.HTTPConnection) or isinstance(obj, http.client.HTTPSConnection):
            conn_info = {
                "host": getattr(obj, "host", "unknown"),
                "port": getattr(obj, "port", "unknown"),
                "timeout": getattr(obj, "timeout", "unknown"),
                "has_sock": hasattr(obj, "sock") and obj.sock is not None,
            }

            # Capture SSL socket details if available
            if hasattr(obj, "sock") and obj.sock is not None and isinstance(obj.sock, ssl.SSLSocket):
                try:
                    sock = obj.sock
                    conn_info["ssl_socket"] = {
                        "cipher": sock.cipher(),
                        "version": sock.version(),
                        "compression": sock.compression(),
                        "pending": sock.pending(),
                        "fileno": sock.fileno() if hasattr(sock, "fileno") else None,
                    }
                except Exception as e:
                    conn_info["ssl_socket_error"] = str(e)

            connection_objects.append(conn_info)

    pool_stats["connection_objects"] = connection_objects

    # Get OS resource info
    try:
        import resource

        rusage = resource.getrusage(resource.RUSAGE_SELF)
        pool_stats["resource_usage"] = {
            "max_rss": rusage.ru_maxrss,
            "page_faults": rusage.ru_minflt,
            "block_input": rusage.ru_inblock,
            "block_output": rusage.ru_oublock,
        }
    except ImportError:
        pool_stats["resource_usage"] = "resource module not available"

    # Get socket statistics if available
    try:
        pool_stats["socket_count"] = len(socket._connection_list) if hasattr(socket, "_connection_list") else "unknown"
    except Exception:
        pool_stats["socket_count"] = "error getting socket count"

    return pool_stats


# Add this function to dump request details directly to the log
def log_full_request_details(request, logger):
    """
    Log complete details of a request including headers and body directly to the logger
    """
    try:
        # Create log sections with clear separation
        logger.error(" ==================== SSL ERROR - FULL REQUEST DUMP ====================")

        # Basic request info
        logger.error(" REQUEST DETAILS:")
        logger.error(f" - Method: {request.method}")
        logger.error(f" - Path: {request.encode_path()}")

        # All headers
        logger.error(" HEADERS:")
        for header_name, header_value in request.headers.items():
            # Mask sensitive headers
            if header_name.lower() in ["authorization", "x-v3io-session-key"]:
                logger.error(f" - {header_name}: [REDACTED]")
            else:
                logger.error(f" - {header_name}: {header_value}")

        # Body content
        if request.body:
            # If body is bytes, decode if possible
            if isinstance(request.body, bytes):
                # try:
                #     body_str = request.body.decode("utf-8")
                #     logger.error(f" {body_str}")
                # except UnicodeDecodeError:
                logger.error(f" [BODY data, length: {len(request.body)} bytes]")
                import base64

                hex_dump = base64.b64encode(request.body[:4096])
                logger.error(f" Hex dump (first 4096 bytes): {hex_dump}")
            elif isinstance(request.body, str):
                logger.error(f" {request.body}")
            else:
                # For file-like objects or other types
                logger.error(f" [Body of type {type(request.body)}, cannot display directly]")
        else:
            logger.error(" [No body]")

        logger.error(" ====================== END OF REQUEST DUMP ======================")
    except Exception as e:
        logger.error(f" Error logging request details: {str(e)}")


class Transport(abstract.Transport):
    _connection_timeout_seconds = 20
    _request_max_retries = 2

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None, verbosity=None):
        super(Transport, self).__init__(logger, endpoint, max_connections, timeout, verbosity)

        self._free_connections = queue.Queue()

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

        # Log initial connection pool state
        pool_stats = get_connection_pool_stats(self)
        self._log(f"Initial connection pool state: {json.dumps(pool_stats)}")

    def close(self):
        # Ignore redundant calls to close
        if not self._free_connections:
            return

        connections = []
        while not self._free_connections.empty():
            conn = self._free_connections.get()
            connections.append(conn)
        # In case anyone tries to reuse this object, we want them to get an error and not hang
        self._free_connections = None
        self._logger.debug(f"Closing all {len(connections)} v3io transport connections")
        for conn in connections:
            conn.close()

    def requires_access_key(self):
        return True

    def send_request(self, request):
        if not self._free_connections:
            raise RuntimeError("Cannot send request on a closed client")

        # TODO: consider getting param of whether we should block or
        #       not (wait for connection to be free or raise exception)
        connection = self._free_connections.get(block=True, timeout=None)

        try:
            return self._send_request_on_connection(request, connection)
        except BaseException as e:
            connection.close()
            connection = self._create_connection(self._host, self._ssl_context)
            self._free_connections.put(connection, block=True)
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
                    request = self._send_request_on_connection(request, connection)
                    connection = request.transport.connection_used

                response = connection.getresponse()
                response_body = response.read()

                status_code, headers = self._get_status_and_headers(response)

                self.log("Rx", connection=connection, status_code=status_code, body=response_body)

                response = v3io.dataplane.response.Response(request.output, status_code, headers, response_body)

                self._free_connections.put(connection, block=True)
                response.raise_for_status(request.raise_for_status or raise_for_status)

                return response

            except v3io.dataplane.response.HttpResponseError as response_error:
                self._logger.warn_with(f"Response error: {response_error}")
                raise response_error
            except BaseException as e:
                connection.close()
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
                    self._free_connections.put(connection, block=True)
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

        # Log request details
        request_info = {
            "method": request.method,
            "path": path,
            "headers": dict(request.headers),
            "body_size": len(request.body) if request.body else 0,
            "connection": {"host": connection.host, "port": connection.port, "timeout": connection.timeout},
        }

        self.log(
            "Tx", connection=connection, method=request.method, path=path, headers=request.headers, body=request.body
        )

        starting_offset = 0
        is_body_seekable = request.body and hasattr(request.body, "seek") and hasattr(request.body, "tell")
        if is_body_seekable:
            starting_offset = request.body.tell()

        retries_left = self._request_max_retries
        while True:
            sock_info_before_request = {}
            try:
                if hasattr(connection, "sock") and connection.sock and isinstance(connection.sock, ssl.SSLSocket):
                    sock = connection.sock
                    sock_info_before_request = {
                        "cipher": sock.cipher(),
                        "version": sock.version(),
                        "compression": sock.compression(),
                        "pending": sock.pending(),
                    }
                connection.request(request.method, path, request.body, request.headers)
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
                connection.close()
                if is_body_seekable:
                    # If the first connection fails, the pointer of the body might move at the size
                    # of the first connection blocksize.
                    # We need to reset the position of the pointer in order to send the whole file.
                    request.body.seek(starting_offset)
                connection = self._create_connection(self._host, self._ssl_context)
                request.transport.connection_used = connection
            except ssl.SSLError as e:
                log_full_request_details(request, self._logger)
                # Detailed SSL error logging
                ssl_error_info = {
                    "error_type": "SSLError",
                    "error_message": str(e),
                    "error_code": e.errno if hasattr(e, "errno") else None,
                    "ssl_lib": e.library if hasattr(e, "library") else None,
                    "ssl_func": e.reason if hasattr(e, "reason") else None,
                    "traceback": traceback.format_exc(),
                    "sock_info_before_request": json.dumps(sock_info_before_request),
                }

                # Get socket state if available
                if hasattr(connection, "sock") and connection.sock:
                    try:
                        sock = connection.sock
                        ssl_error_info["socket_state"] = {
                            "fileno": sock.fileno() if hasattr(sock, "fileno") else None,
                            "blocking": sock.getblocking() if hasattr(sock, "getblocking") else None,
                            "timeout": sock.gettimeout() if hasattr(sock, "gettimeout") else None,
                        }
                    except Exception as sock_e:
                        ssl_error_info["socket_state_error"] = str(sock_e)

                # Get connection pool stats
                try:
                    ssl_error_info["pool_stats"] = get_connection_pool_stats(self)
                except Exception as pool_e:
                    ssl_error_info["pool_stats_error"] = str(pool_e)

                ssl_error_info["request_info"] = request_info

                self._logger.error(f" SSL Error: {json.dumps(ssl_error_info)}")
                raise e
            except BaseException as e:
                self._logger.error_with(
                    "Unhandled exception while sending request", e=type(e), e_msg=e, connection=connection
                )
                raise e

        return request

    def _create_connections(self, num_connections, host, ssl_context):
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
