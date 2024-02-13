import itertools

import pytest

import v3io.dataplane.client
import v3io.dataplane.transport.httpclient
from v3io.dataplane.object import Model
from v3io.dataplane.response import HttpResponseError


class MockResponse:
    def __init__(self, code=None, headers=None):
        self.code = code or 200
        self.headers = headers or {}

    def read(self):
        return "mock response"

    def getheaders(self):
        return self.headers


class ATestException(Exception):
    pass


class MockConnection:
    def __init__(self, fail_request_after=None, fail_getresponse_after=None):
        self.times_closed = 0
        self.times_request = 0
        self.times_got_response = 0

        self.fail_request_after = fail_request_after
        self.fail_getresponse_after = fail_getresponse_after

    def request(self, method, path, body, headers):
        self.times_request += 1
        if self.fail_request_after is not None and self.times_request > self.fail_request_after:
            raise ATestException(f"Failing request number {self.times_request}")

    def getresponse(self):
        self.times_got_response += 1
        if self.fail_getresponse_after is not None and self.times_got_response > self.fail_getresponse_after:
            raise ATestException(f"Failing response number {self.times_got_response}")
        return MockResponse()

    def raise_for_status(self, expected_statuses=None):
        pass

    def close(self):
        self.times_closed += 1


class MockTransport(v3io.dataplane.transport.httpclient.Transport):
    def __init__(self, *args, connection_options=None, **kwargs):
        self.mock_connections = []
        self.connection_options = connection_options or {}
        super().__init__(*args, **kwargs)

    def _create_connection(self, host, ssl_context):
        conn = MockConnection(**self.connection_options)
        self.mock_connections.append(conn)
        return conn


def test_connection_creation_and_close():
    client = v3io.dataplane.Client()
    mock_transport = MockTransport(client._logger)
    client._transport = mock_transport
    for _ in range(20):
        client.get_object("mycontainer", "mypath")
    assert len(mock_transport.mock_connections) == 8
    for conn in mock_transport.mock_connections:
        assert conn.times_closed == 0
    client.close()
    assert len(mock_transport.mock_connections) == 8
    for conn in mock_transport.mock_connections:
        assert conn.times_closed == 1


@pytest.mark.parametrize(["fail_request_after", "fail_getresponse_after"], itertools.permutations(range(3), 2))
def test_connection_closed_on_error(fail_request_after, fail_getresponse_after):
    client = v3io.dataplane.Client()
    connection_options = {
        "fail_request_after": fail_request_after,
        "fail_getresponse_after": fail_getresponse_after,
    }
    mock_transport = MockTransport(client._logger, connection_options=connection_options)
    client._transport = mock_transport
    for _ in range(20):
        try:
            client.get_object("mycontainer", "mypath")
        except ATestException:
            pass
    num_connections_still_open = 0
    for conn in mock_transport.mock_connections:
        assert conn.times_closed == 0 or conn.times_closed == 1
        if conn.times_closed == 0:
            num_connections_still_open += 1
    assert num_connections_still_open == 8
    client.close()
    for conn in mock_transport.mock_connections:
        assert conn.times_closed == 1


def test_error_on_use_after_close():
    client = v3io.dataplane.Client()
    client.object.get("doesntexist", "doesntexist", raise_for_status=v3io.dataplane.RaiseForStatus.never)
    client.close()
    with pytest.raises(RuntimeError):
        client.object.get("doesntexist", "doesntexist", raise_for_status=v3io.dataplane.RaiseForStatus.never)


@pytest.mark.parametrize("object_function", [Model.get, Model.put, Model.delete])
def test_raise_http_response_error(object_function):
    client = v3io.dataplane.Client(
        transport_kind="httpclient",
    )
    with pytest.raises(HttpResponseError, match="Container not found") as response_error:
        object_function(client.object, "not-exists", "path/to/object")
    assert response_error.value.status_code == 404
