import ujson

import future.utils

import v3io.common.helpers


class Input(object):

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


class GetObjectInput(Input):

    def __init__(self, path, offset=None, num_bytes=None):
        self.path = path
        self.offset = offset
        self.num_bytes = num_bytes

    def encode(self, container_name, access_key):
        return self._encode('GET', container_name, access_key, self.path, None, None)


class PutObjectInput(Input):

    def __init__(self, path, offset, body):
        self.path = path
        self.offset = offset
        self.body = body

    def encode(self, container_name, access_key):
        return self._encode('PUT', container_name, access_key, self.path, None, self.body)


class DeleteObjectInput(Input):

    def __init__(self, path):
        self.path = path

    def encode(self, container_name, access_key):
        return self._encode('DELETE', container_name, access_key, self.path, None, None)


class PutItemInput(Input):

    def __init__(self, path, attributes, condition=None):
        self.path = path
        self.attributes = attributes
        self.condition = condition

    def encode(self, container_name, access_key):

        # add 'Item' to body
        body = {
            'Item': self._dict_to_typed_attributes(self.attributes)
        }

        if self.condition is not None:
            body['ConditionExpression'] = self.condition

        return self._encode('PUT',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'PutItem'},
                            body)


class PutItemsInput(Input):

    def __init__(self, path, items, condition=None):
        self.path = path
        self.items = items
        self.condition = condition


class UpdateItemInput(Input):

    def __init__(self, path, attributes=None, expression=None, condition=None):
        self.path = path
        self.attributes = attributes
        self.expression = expression
        self.condition = condition

    def encode(self, container_name, access_key):

        # add 'Item' to body
        body = {
            'UpdateMode': 'CreateOrReplaceAttributes'
        }

        if self.condition is not None:
            body['ConditionExpression'] = self.condition

        if not self.expression and not self.attributes:
            raise RuntimeError('One of expression or attributes must be populated for update item')

        if self.expression:
            http_method = 'POST'
            function_name = 'UpdateItem'
            body['UpdateExpression'] = self.expression

        elif self.attributes:
            http_method = 'PUT'
            function_name = 'PutItem'
            body['Item'] = self._dict_to_typed_attributes(self.attributes)

        return self._encode(http_method,
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': function_name},
                            body)


class GetItemInput(Input):

    def __init__(self, path, attribute_names='*'):
        self.path = path
        self.attribute_names = attribute_names

    def encode(self, container_name, access_key):

        # add 'Item' to body
        body = {
            'AttributesToGet': ','.join(self.attribute_names)
        }

        return self._encode('PUT',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'GetItem'},
                            body)


class GetItemsInput(Input):

    def __init__(self,
                 path,
                 attribute_names='*',
                 filter_expression=None,
                 marker=None,
                 sharding_key=None,
                 limit=None,
                 segment=None,
                 total_segments=None,
                 sort_key_range_start=None,
                 sort_key_range_end=None):
        self.path = path
        self.attribute_names = attribute_names
        self.filter_expression = filter_expression
        self.marker = marker
        self.sharding_key = sharding_key
        self.limit = limit
        self.segment = segment
        self.total_segments = total_segments
        self.sort_key_range_start = sort_key_range_start
        self.sort_key_range_end = sort_key_range_end

    def encode(self, container_name, access_key):
        body = {
            'AttributesToGet': ','.join(self.attribute_names),
        }

        if self.filter_expression:
            body['FilterExpression'] = self.filter_expression

        if self.marker:
            body['Marker'] = self.marker

        if self.sharding_key:
            body['ShardingKey'] = self.sharding_key

        if self.limit:
            body['Limit'] = self.limit

        if self.segment:
            body['Segment'] = self.segment

        if self.total_segments:
            body['TotalSegment'] = self.total_segments

        if self.sort_key_range_start:
            body['SortKeyRangeStart'] = self.sort_key_range_start

        if self.sort_key_range_end:
            body['SortKeyRangeEnd'] = self.sort_key_range_end

        return self._encode('PUT',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'GetItems'},
                            body)
