import ujson

import future.utils

import v3io.common.helpers


class RequestEncoder(object):

    def __init__(self):
        pass

    def encode_get_object(self, container_name, access_key, path, offset=None, num_bytes=None):
        return self._encode('GET', container_name, access_key, path, None, None)

    def encode_put_object(self, container_name, access_key, path, offset, body):
        return self._encode('PUT', container_name, access_key, path, None, body)

    def encode_delete_object(self, container_name, access_key, path):
        return self._encode('DELETE', container_name, access_key, path, None, None)

    def encode_put_item(self, container_name, access_key, path, attributes):

        # add 'Item' to body
        body = {
            'Item': self._dict_to_typed_attributes(attributes)
        }

        return self._encode('PUT',
                            container_name,
                            access_key,
                            path,
                            {'X-v3io-function': 'PutItem'},
                            body)

    def encode_update_item(self, container_name, access_key, path, attributes, expression):

        # add 'Item' to body
        body = {
            'UpdateMode': 'CreateOrReplaceAttributes'
        }

        if expression:
            http_method = 'POST'
            function_name = 'UpdateItem'
            body['UpdateExpression'] = expression

        if attributes:
            http_method = 'PUT'
            function_name = 'PutItem'
            body['Item'] = self._dict_to_typed_attributes(attributes)

        return self._encode(http_method,
                            container_name,
                            access_key,
                            path,
                            {'X-v3io-function': function_name},
                            body)

    def encode_get_item(self, container_name, access_key, path, attribute_names):

        # add 'Item' to body
        body = {
            'AttributesToGet': ','.join(attribute_names)
        }

        return self._encode('PUT',
                            container_name,
                            access_key,
                            path,
                            {'X-v3io-function': 'GetItem'},
                            body)

    def encode_get_items(self,
                         container_name,
                         access_key,
                         path,
                         attribute_names,
                         filter_expression,
                         marker,
                         sharding_key,
                         limit,
                         segment,
                         total_segments):

        body = {
            'AttributesToGet': ','.join(attribute_names),
        }

        if filter_expression:
            body['FilterExpression'] = filter_expression

        if marker:
            body['Marker'] = marker

        if sharding_key:
            body['ShardingKey'] = sharding_key

        if limit:
            body['Limit'] = limit

        if segment:
            body['Segment'] = segment

        if total_segments:
            body['TotalSegment'] = total_segments

        return self._encode('PUT',
                            container_name,
                            access_key,
                            path,
                            {'X-v3io-function': 'GetItems'},
                            body)

    def _encode(self, method, container_name, access_key, path, headers, body):
        path = self._resolve_path(container_name, path)
        headers, body = self._resolve_body_and_headers(access_key, headers, body)

        return method, path, headers, body

    def _typed_attributes_to_dict(self):
        pass

    def _dict_to_typed_attributes(self, d):
        typed_attributes = {}

        for (key, value) in future.utils.viewitems(d):
            attribute_type = type(value)

            if isinstance(value, future.utils.string_types):
                type_key = 'S'
            elif attribute_type in [int, float]:
                type_key = 'N'
            elif attribute_type in [bytes, bytearray]:
                type_key = 'B'
            else:
                raise AttributeError('Attribute {0} has unsupported type {1}'.format(key, attribute_type))

            typed_attributes[key] = {type_key: str(value)}

        return typed_attributes

    def _resolve_body_and_headers(self, access_key, headers, body):
        if access_key:
            headers = headers or {}
            headers['X-v3io-session-key'] = access_key

        if not isinstance(body, dict):
            return headers, body

        body = ujson.dumps(body)
        headers['Content-Type'] = 'application/json'

        return headers, body

    def _resolve_path(self, container_name, path):
        return v3io.common.helpers.url_join(container_name, path)
