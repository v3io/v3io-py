import future.utils


class Output(object):

    def _decode_typed_attributes(self, typed_attributes):
        decoded_attributes = {}

        for attribute_key, typed_attribute_value in future.utils.viewitems(typed_attributes):
            for attribute_type, attribute_value in future.utils.viewitems(typed_attribute_value):
                if attribute_type == 'N':
                    decoded_attribute = float(attribute_value)
                elif attribute_type == 'B':
                    decoded_attribute = bytearray(attribute_value)
                else:
                    decoded_attribute = attribute_value

                decoded_attributes[attribute_key] = decoded_attribute

        return decoded_attributes

    def _get_child_text(self, child, child_key, target_dict, target_dict_key):
        child_value = child.find(child_key)
        if child_value is not None:
            target_dict[target_dict_key] = child_value.text


#
# Containers
#

class GetContainersOutput(Output):
    def __init__(self, root):
        self.containers = []

        for bucket in root.find('Buckets'):
            self.containers.append({
                'name': bucket.find('Name').text,
                'creation_date': bucket.find('CreationDate').text,
                'id': int(bucket.find('Id').text),
            })


class GetContainerContentsOutput(Output):
    def __init__(self, root):
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
                content = {}

                # populate the fields
                for field in [
                    ('Key', 'key'),
                    ('Size', 'size'),
                    ('LastSequenceID', 'last_sequence_id'),
                    ('LastModified', 'last_modified'),
                    ('Mode', 'mode'),
                    ('AccessTime', 'access_time'),
                    ('CreatingTime', 'creating_time'),
                    ('GID', 'gid'),
                    ('UID', 'uid'),
                    ('InodeNumber', 'inode_number')
                ]:
                    self._get_child_text(content_child, field[0], content, field[1])

                self.contents.append(content)

        if common_prefixes_children is not None:
            for common_prefix_child in common_prefixes_children:
                common_prefix = {}

                # populate the fields
                for field in [
                    ('Prefix', 'prefix'),
                    ('LastModified', 'last_modified'),
                    ('AccessTime', 'access_time'),
                    ('CreatingTime', 'creating_time'),
                    ('Mode', 'mode'),
                    ('GID', 'gid'),
                    ('UID', 'uid'),
                    ('InodeNumber', 'inode_number')
                ]:
                    self._get_child_text(common_prefix_child, field[0], common_prefix, field[1])

                self.common_prefixes.append(common_prefix)

#
# KV/EMD
#


class GetItemOutput(Output):

    def __init__(self, decoded_body):
        self.item = self._decode_typed_attributes(decoded_body.get('Item', {}))


class GetItemsOutput(Output):

    def __init__(self, decoded_body):
        self.last = decoded_body['LastItemIncluded'] == 'TRUE'
        self.next_marker = decoded_body.get('NextMarker')
        self.items = []

        for item in decoded_body['Items']:
            self.items.append(self._decode_typed_attributes(item))


#
# Stream
#

class DescribeStreamOutput(Output):

    def __init__(self, decoded_body):
        self.shard_count = decoded_body['ShardCount']
        self.retention_period_hours = decoded_body.get('RetentionPeriodHours')


class SeekShardOutput(Output):

    def __init__(self, decoded_body):
        self.location = decoded_body['Location']


class PutRecordsOutput(Output):

    def __init__(self, decoded_body):
        self.failed_record_count = decoded_body['FailedRecordCount']
        self.records = []

        for record in decoded_body['Records']:
            self.records.append({
                'sequence_number': record['SequenceNumber'],
                'shard_id': record['ShardId'],
                'error_code': record.get('ErrorCode'),
                'error_message': record.get('ErrorMessage')
            })


class GetRecordsOutput(Output):

    def __init__(self, decoded_body):
        self.next_location = decoded_body['NextLocation']
        self.msec_behind_latest = decoded_body['MSecBehindLatest']
        self.records_behind_latest = decoded_body['RecordsBehindLatest']
        self.records = []

        for record in decoded_body['Records']:
            self.records.append({
                'arrival_time_sec': record.get('ArrivalTimeSec'),
                'arrival_time_nsec': record.get('ArrivalTimeNSec'),
                'sequence_number': record.get('SequenceNumber'),
                'client_info': record.get('ClientInfo'),
                'partition_key': record.get('PartitionKey'),
                'data': record.get('Data'),
            })
