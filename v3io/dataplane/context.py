import os

import future.utils
import requests

import v3io.dataplane.v3io_api
import v3io.common.helpers


class Context(object):

    def __init__(self, logger, endpoints=None, max_connections=4):
        self._logger = logger
        self._endpoints = self._get_endpoints(endpoints)
        self._next_connection_pool = 0
        self._request_encoder = v3io.dataplane.v3io_api.RequestEncoder()

        # create a tuple of connection pools
        self._connection_pools = self._create_connection_pools(self._endpoints, max_connections)

    def get_object(self, container_name, access_key, path, offset=None, num_bytes=None):
        return self._encode_and_http_request(locals(),
                                             self._request_encoder.encode_get_object,
                                             v3io.dataplane.v3io_api.Response)

    def put_object(self, container_name, access_key, path, offset, body):
        return self._encode_and_http_request(locals(),
                                             self._request_encoder.encode_put_object,
                                             v3io.dataplane.v3io_api.Response)

    def delete_object(self, container_name, access_key, path):
        return self._encode_and_http_request(locals(),
                                             self._request_encoder.encode_delete_object,
                                             v3io.dataplane.v3io_api.Response)

    def put_item(self, container_name, access_key, path, attributes):
        return self._encode_and_http_request(locals(),
                                             self._request_encoder.encode_put_item,
                                             v3io.dataplane.v3io_api.Response)

    def put_items(self, container_name, access_key, path, items):
        put_items_response = v3io.dataplane.v3io_api.PutItemsResponse()

        for item_path, item_attributes in future.utils.viewitems(items):
            method, encoded_path, headers, body = self._request_encoder.encode_put_item(container_name,
                                                                                        access_key,
                                                                                        v3io.common.helpers.url_join(path, item_path),
                                                                                        item_attributes)

            put_items_response.add_response(self._http_request(method, encoded_path, headers, body))

        return put_items_response

    def update_item(self, container_name, access_key, path, attributes=None, expression=None):
        return self._encode_and_http_request(locals(),
                                             self._request_encoder.encode_update_item,
                                             v3io.dataplane.v3io_api.Response)

    def get_item(self, container_name, access_key, path, attribute_names=None):
        return self._encode_and_http_request(locals(),
                                             self._request_encoder.encode_get_item,
                                             v3io.dataplane.v3io_api.GetItemResponse)

    def get_items(self,
                  container_name,
                  access_key,
                  path,
                  attribute_names=None,
                  filter_expression=None,
                  marker=None,
                  sharding_key=None,
                  limit=None,
                  segment=None,
                  total_segments=None):
        return self._encode_and_http_request(locals(),
                                             self._request_encoder.encode_get_items,
                                             v3io.dataplane.v3io_api.GetItemsResponse)

    def _http_request(self, method, path, headers=None, body=None):
        endpoint, connection_pool = self._get_next_connection_pool()

        self._logger.debug_with('Tx', method=method, path=path, headers=headers, body=body)

        response = connection_pool.request(method, endpoint + path, headers=headers, data=body)

        self._logger.debug_with('Rx', status_code=response.status_code, headers=response.headers, body=response.text)

        return response

    def _get_endpoints(self, endpoints):
        if endpoints is not None:
            return endpoints

        env_endpoints = os.environ.get('V3IO_API')

        if env_endpoints is not None:
            return env_endpoints.split(',')

        raise RuntimeError('Endpoints must be passed to context or specified in V3IO_API')

    def _create_connection_pools(self, endpoints, max_connections):
        connection_pools = []

        for endpoint in endpoints:
            connection_pools.append((endpoint, requests.Session()))

        return tuple(connection_pools)

    def _get_next_connection_pool(self):

        # TODO: multithreading safe
        endpoint, connection_pool = self._connection_pools[self._next_connection_pool]

        self._next_connection_pool += 1
        if self._next_connection_pool >= len(self._connection_pools):
            self._next_connection_pool = 0

        return endpoint, connection_pool

    def _encode_and_http_request(self, args, encoder, decoder):
        del args['self']

        # get request params with the encoder
        method, path, headers, body = encoder(**args)

        # call the encoder to get the response
        response = self._http_request(method, path, headers, body)

        # call the decoder to decode the response
        return decoder(response.status_code, headers, response.text)
