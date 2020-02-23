import os
import requests

import v3io.dataplane.response


class RaiseForStatus(object):
    never = 'never'
    always = 'always'


class Transport(object):

    def __init__(self, logger, endpoints=None, max_connections=4, timeout=None):
        self._logger = logger
        self._endpoints = self._get_endpoints(endpoints)
        self._next_connection_pool = 0
        self._timeout = timeout

        # create a tuple of connection pools
        self._connection_pools = self._create_connection_pools(self._endpoints, max_connections)

    def close(self):
        for (endpoint, session) in self._connection_pools:
            session.close()

    def encode_and_send(self,
                        container_name,
                        access_key,
                        raise_for_status,
                        encoder,
                        encoder_args,
                        output=None):

        # get request params with the encoder
        method, path, headers, body = encoder(container_name, access_key, encoder_args)

        # call the encoder to get the response
        http_response = self._http_request(method, path, headers, body)

        # create a response
        response = v3io.dataplane.response.Response(output,
                                                    http_response.status_code,
                                                    headers,
                                                    http_response.text)

        # if user didn't specify never to raise, raise for the given statuses
        if raise_for_status != RaiseForStatus.never:
            response.raise_for_status(raise_for_status)

        return response

    def _http_request(self, method, path, headers=None, body=None):
        endpoint, connection_pool = self._get_next_connection_pool()

        # self._logger.debug_with('Tx', method=method, path=path, headers=headers, body=body)

        response = connection_pool.request(method,
                                           endpoint + path,
                                           headers=headers,
                                           data=body,
                                           timeout=self._timeout,
                                           verify=False)

        # self._logger.debug_with('Rx', status_code=response.status_code, headers=response.headers, body=response.text)

        return response

    def _get_endpoints(self, endpoints):
        if endpoints is None:
            env_endpoints = os.environ.get('V3IO_API')

            if env_endpoints is not None:
                endpoints = env_endpoints.split(',')
            else:
                raise RuntimeError('Endpoints must be passed to context or specified in V3IO_API')

        endpoints_with_scheme = []

        for endpoint in endpoints:
            if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
                endpoints_with_scheme.append('http://' + endpoint)
            else:
                endpoints_with_scheme.append(endpoint)

        return endpoints_with_scheme

    def _create_connection_pools(self, endpoints, max_connections):
        connection_pools = []

        for endpoint in endpoints:
            connection_pools.append((endpoint, requests.Session()))

        return tuple(connection_pools)

    def _get_next_connection_pool(self):

        # TODO: multithreading safe way to get a connection pool. for now
        # support only one target
        return self._connection_pools[0]
