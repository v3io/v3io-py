import base64

import future.utils


class Output(object):

    def _decode_typed_attributes(self, typed_attributes):
        decoded_attributes = {}

        for attribute_key, typed_attribute_value in future.utils.viewitems(typed_attributes):
            for attribute_type, attribute_value in future.utils.viewitems(typed_attribute_value):
                if attribute_type == 'N':
                    try:
                        decoded_attribute = int(attribute_value)
                    except ValueError:
                        decoded_attribute = float(attribute_value)
                elif attribute_type == 'B':
                    decoded_attribute = base64.b64decode(attribute_value)
                elif attribute_type == 'S':
                    decoded_attribute = str(attribute_value)
                else:
                    decoded_attribute = attribute_value

                decoded_attributes[attribute_key] = decoded_attribute

        return decoded_attributes


#
# Containers
#

class Container(object):

    def __init__(self, name, creation_date, identifier):
        self.name = name
        self.creation_date = creation_date
        self.identifier = identifier


class GetContainersOutput(Output):

    def __init__(self, root):

        # got an error code
        if isinstance(root, dict):
            self.error = root
            return

        self.containers = []

        for bucket in root.find('Buckets'):
            self.containers.append(Container(
                name=bucket.find('Name').text,
                creation_date=bucket.find('CreationDate').text,
                identifier=int(bucket.find('Id').text)
            ))


class ContainerContent(object):

    def __init__(self, child):

        # got an error code
        if isinstance(child, dict):
            self.error = child
            return

        for child_key, attribute_name, kind in [
            ('Key', 'key', str),
            ('Size', 'size', int),
            ('LastSequenceID', 'last_sequence_id', int),
            ('LastModified', 'last_modified', str),
            ('Mode', 'mode', str),
            ('AccessTime', 'access_time', str),
            ('CreatingTime', 'creating_time', str),
            ('GID', 'gid', str),
            ('UID', 'uid', str),
            ('InodeNumber', 'inode_number', int)
        ]:
            child_value = child.find(child_key)
            if child_value is not None:
                setattr(self, attribute_name, kind(child_value.text))


class ContainerCommonPrefix(object):

    def __init__(self, child):

        # got an error code
        if isinstance(child, dict):
            self.error = child
            return

        for child_key, attribute_name, kind in [
            ('Prefix', 'prefix', str),
            ('LastModified', 'last_modified', str),
            ('AccessTime', 'access_time', str),
            ('CreatingTime', 'creating_time', str),
            ('Mode', 'mode', str),
            ('GID', 'gid', str),
            ('UID', 'uid', str),
            ('InodeNumber', 'inode_number', int)
        ]:
            child_value = child.find(child_key)
            if child_value is not None:
                setattr(self, attribute_name, kind(child_value.text))


class GetContainerContentsOutput(Output):

    def __init__(self, root):

        # got an error code
        if isinstance(root, dict):
            self.error = root
            return

        self.name = root.find('Name').text
        self.next_marker = root.find('NextMarker').text
        self.max_keys = root.find('MaxKeys').text
        self.is_truncated = root.find('IsTruncated').text
        self.contents = []
        self.common_prefixes = []

        contents_children = root.findall('Contents')
        common_prefixes_children = root.findall('CommonPrefixes')

        if contents_children is not None:
            for content_child in contents_children:
                self.contents.append(ContainerContent(content_child))

        if common_prefixes_children is not None:
            for common_prefix_child in common_prefixes_children:
                self.common_prefixes.append(ContainerCommonPrefix(common_prefix_child))


#
# KV/EMD
#


class GetItemOutput(Output):

    def __init__(self, decoded_body):
        self.item = self._decode_typed_attributes(decoded_body.get('Item', {}))


class GetItemsOutput(Output):

    def __init__(self, decoded_body):
        self.last = decoded_body.get('LastItemIncluded') == 'TRUE'
        self.next_marker = decoded_body.get('NextMarker')
        self.items = []

        for item in decoded_body.get('Items', []):
            self.items.append(self._decode_typed_attributes(item))


#
# Stream
#

class DescribeStreamOutput(Output):

    def __init__(self, decoded_body):
        self.shard_count = decoded_body.get('ShardCount')
        self.retention_period_hours = decoded_body.get('RetentionPeriodHours')


class SeekShardOutput(Output):

    def __init__(self, decoded_body):
        self.location = decoded_body.get('Location')


class PutRecordsResult(Output):

    def __init__(self, decoded_body):
        self.sequence_number = decoded_body.get('SequenceNumber')
        self.shard_id = decoded_body.get('ShardId')
        self.error_code = decoded_body.get('ErrorCode')
        self.error_message = decoded_body.get('ErrorMessage')


class PutRecordsOutput(Output):

    def __init__(self, decoded_body):
        self.failed_record_count = decoded_body.get('FailedRecordCount')
        self.records = []

        for record in decoded_body.get('Records', []):
            self.records.append(PutRecordsResult(record))


class GetRecordsResult(Output):

    def __init__(self, decoded_body):
        self.arrival_time_sec = decoded_body.get('ArrivalTimeSec')
        self.arrival_time_nsec = decoded_body.get('ArrivalTimeNSec')
        self.sequence_number = decoded_body.get('SequenceNumber')
        self.client_info = decoded_body.get('ClientInfo')
        self.partition_key = decoded_body.get('PartitionKey')
        self.data = self._from_base64(decoded_body.get('Data'))

    @staticmethod
    def _from_base64(value):
        if value is None:
            return None

        return base64.b64decode(value)


class GetRecordsOutput(Output):

    def __init__(self, decoded_body):
        self.next_location = decoded_body.get('NextLocation')
        self.msec_behind_latest = decoded_body.get('MSecBehindLatest')
        self.records_behind_latest = decoded_body.get('RecordsBehindLatest')
        self.records = []

        for record in decoded_body.get('Records'):
            self.records.append(GetRecordsResult(record))
