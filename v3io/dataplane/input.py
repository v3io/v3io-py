import ujson
import base64
import urllib.parse

import future.utils

import v3io.common.helpers


class Input(object):

    def _encode(self, method, container_name, access_key, path, headers, body):
        if container_name:
            path = self._resolve_path(container_name, path)
        else:
            path = path

        headers, body = self._resolve_body_and_headers(access_key, headers, body)

        return method, path, headers, body

    def _typed_attributes_to_dict(self):
        pass

    def _ensure_trailing_slash(self, path):
        if not path.endswith('/'):
            return path + '/'

        return path

    def _to_base64(self, input):
        if isinstance(input, str):
            input = input.encode('utf-8')

        return base64.b64encode(input)

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

#
# Containers
#


class GetContainersInput(Input):

    def __init__(self):
        pass

    def encode(self, container_name, access_key):
        return self._encode('GET',
                            None,
                            access_key,
                            '/',
                            {},
                            None)


class GetContainerContentsInput(Input):

    def __init__(self,
                 path,
                 get_all_attributes=False,
                 directories_only=False,
                 limit=None,
                 marker=None):
        self.path = path
        self.get_all_attributes = get_all_attributes
        self.directories_only = directories_only
        self.limit = limit
        self.marker = marker

    def encode(self, container_name, access_key):
        query = {
            'prefix': self.path
        }

        if self.get_all_attributes:
            query['prefix-info'] = 1

        if self.directories_only:
            query['prefix-only'] = 1

        if self.limit:
            query['max-keys'] = self.limit

        if self.marker:
            query['marker'] = self.marker

        return self._encode('GET',
                            None,
                            access_key,
                            '/{0}?{1}'.format(container_name, urllib.parse.urlencode(query)),
                            {},
                            None)

#
# Object
#


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


#
# KV/EMD
#


class PutItemInput(Input):

    def __init__(self, path, attributes, condition=None, update_mode=None):
        self.path = path
        self.attributes = attributes
        self.condition = condition
        self.update_mode = update_mode

    def encode(self, container_name, access_key):
        # add 'Item' to body
        body = {
            'Item': self._dict_to_typed_attributes(self.attributes)
        }

        if self.condition is not None:
            body['ConditionExpression'] = self.condition

        if self.update_mode is not None:
            body['UpdateMode'] = self.update_mode

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

    def __init__(self, path, attributes=None, expression=None, condition=None, update_mode=None):
        self.path = path
        self.attributes = attributes
        self.expression = expression
        self.condition = condition
        self.update_mode = update_mode or 'CreateOrReplaceAttributes'

    def encode(self, container_name, access_key):

        # add 'Item' to body
        body = {
            'UpdateMode': self.update_mode
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
                 table_name=None,
                 attribute_names='*',
                 filter=None,
                 marker=None,
                 sharding_key=None,
                 limit=None,
                 segment=None,
                 total_segments=None,
                 sort_key_range_start=None,
                 sort_key_range_end=None):
        self.path = path
        self.table_name = table_name
        self.attribute_names = attribute_names
        self.filter = filter
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

        if self.table_name:
            body['TableName'] = self.table_name

        if self.filter:
            body['FilterExpression'] = self.filter

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

#
# Stream
#


class CreateStreamInput(Input):

    def __init__(self, path, shard_count, retention_period_hours=None):
        self.path = self._ensure_trailing_slash(path)
        self.shard_count = shard_count
        self.retention_period_hours = retention_period_hours or 1

    def encode(self, container_name, access_key):
        body = {
            'ShardCount': self.shard_count,
            'RetentionPeriodHours': self.retention_period_hours
        }

        return self._encode('POST',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'CreateStream'},
                            body)


class DescribeStreamInput(Input):

    def __init__(self, path):
        self.path = self._ensure_trailing_slash(path)

    def encode(self, container_name, access_key):
        return self._encode('PUT',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'DescribeStream'},
                            None)


class SeekShardInput(Input):

    def __init__(self, path, seek_type, starting_sequence_number=None, timestamp=None):
        self.path = self._ensure_trailing_slash(path)
        self.seek_type = seek_type
        self.starting_sequence_number = starting_sequence_number
        self.timestamp = timestamp

    def encode(self, container_name, access_key):
        body = {
            'Type': self.seek_type,
        }

        if self.seek_type == 'SEQUENCE':
            body['StartingSequenceNumber'] = self.starting_sequence_number
        elif self.seek_type == 'TIME':
            body['TimestampSec'] = self.timestamp
            body['TimestampNSec'] = 0
        elif self.seek_type not in ['EARLIEST', 'LATEST'] :
            raise ValueError('Unsupported seek_type ({0}) for seek_shard. Must be one of SEQUENCE, TIME, EARLIEST, LATEST'.
                             format(self.seek_type))

        return self._encode('PUT',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'SeekShard'},
                            body)


class PutRecordsInput(Input):

    def __init__(self, path, records):
        self.path = self._ensure_trailing_slash(path)
        self.records = records

    def encode(self, container_name, access_key):
        records = []

        for record in self.records:
            record_body = {
                'Data': self._to_base64(record['data']),
            }

            try:
                record_body['ClientInfo'] = self._to_base64(record['client_info'])
            except KeyError:
                pass

            try:
                record_body['ShardId'] = record['shard_id']
            except KeyError:
                pass

            try:
                record_body['PartitionKey'] = record['partition_key']
            except KeyError:
                pass

            records.append(record_body)

        body = {
            'Records': records
        }

        return self._encode('POST',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'PutRecords'},
                            body)


class GetRecordsInput(Input):

    def __init__(self, path, location, limit=None):
        self.path = self._ensure_trailing_slash(path)
        self.location = location
        self.limit = limit or 100

    def encode(self, container_name, access_key):
        body = {
            'Location': self.location,
            'Limit': self.limit,
        }

        return self._encode('PUT',
                            container_name,
                            access_key,
                            self.path,
                            {'X-v3io-function': 'GetRecords'},
                            body)



