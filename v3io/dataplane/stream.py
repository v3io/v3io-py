import os

import v3io.dataplane.request
import v3io.dataplane.output
import v3io.dataplane.model
import v3io.dataplane.kv_cursor


class Model(v3io.dataplane.model.Model):

    def __init__(self, client):
        self._client = client
        self._access_key = client._access_key
        self._transport = client._transport

    def create(self,
               container,
               stream_path,
               shard_count,
               access_key=None,
               raise_for_status=None,
               transport_actions=None,
               retention_period_hours=None):
        """Creates and configures a new stream. The configuration includes the stream's shard count and retention
        period. The new stream is available immediately upon its creation.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/streaming-web-api/createstream/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        stream_path (Required) : str
            A unique name for the new stream (collection) that will be created.
        shard_count (Required) : int
            The steam's shard count, i.e., the number of stream shards to create.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        retention_period_hours (Optional) : int
            The stream's retention period, in hours. After this period elapses, when new records are added to the
            stream, the earliest ingested records are deleted. default: 24

        Return Value
        ----------
        A `Response` object.
        """
        stream_path = self._ensure_path_ends_with_slash(stream_path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_create_stream,
                                       locals())

    def update(self,
               container,
               stream_path,
               shard_count,
               access_key=None,
               raise_for_status=None,
               transport_actions=None):
        """Updates a stream's configuration by increasing its shard count. The changes are applied immediately.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/streaming-web-api/updatestream/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        stream_path (Required) : str
            A unique name for the new stream (collection) that will be created.
        shard_count (Required) : int
            The steam's shard count, i.e., the number of stream shards to create.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object.
        """
        stream_path = self._ensure_path_ends_with_slash(stream_path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_update_stream,
                                       locals())

    def delete(self, container, stream_path, access_key=None, raise_for_status=None):
        """Deletes a stream object along with all of its shards.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        stream_path (Required) : str
            The stream_path of the stream.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object.
        """
        stream_path = self._ensure_path_ends_with_slash(stream_path)

        response = self._client.container.list(container,
                                               stream_path,
                                               access_key,
                                               raise_for_status)

        # nothing to do
        if response.status_code == 404:
            return response

        for stream_shard in response.output.contents:
            self._client.object.delete(container, stream_shard.key, access_key, raise_for_status)

        return self._client.object.delete(container, stream_path, access_key, raise_for_status)

    def describe(self, container, stream_path, access_key=None, raise_for_status=None, transport_actions=None):
        """Retrieves a stream's configuration, including the shard count and retention period.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/streaming-web-api/describestream/.

        Parameters
        ----------
        stream_path (Required) : str
            The stream_path of the stream.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object, whose `output` is `DescribeStreamOutput`.
        """
        stream_path = self._ensure_path_ends_with_slash(stream_path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_describe_stream,
                                       locals(),
                                       v3io.dataplane.output.DescribeStreamOutput)

    def seek(self,
             container,
             stream_path,
             shard_id,
             seek_type,
             access_key=None,
             raise_for_status=None,
             transport_actions=None,
             starting_sequence_number=None,
             timestamp_sec=None,
             timestamp_nsec=None):
        """Returns the requested location within the specified stream shard, for use in a subsequent GetRecords
        operation. The operation supports different seek types, as outlined in the Stream Record Consumption
        overview and in the description of the Type request parameter below.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/streaming-web-api/seek/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        stream_path (Required) : str
            The stream_path of the stream.
        seek_type (Required) : str
            'EARLIEST': the location of the earliest ingested record in the shard.
            'LATEST': the location of the end of the shard.
            'TIME': the location of the earliest ingested record in the shard beginning at the base time set in the
                    TimestampSec and TimestampNSec request parameters. If no matching record is found (i.e., if all
                    records in the shard arrived before the specified base time) the operation returns the location
                    of the end of the shard.
            'SEQUENCE': the location of the record whose sequence number matches the sequence number specified in the
                        StartingSequenceNumber request parameter. If no match is found, the operation fails.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        starting_sequence_number (Optional) : int
            Record sequence number for a sequence-number based seek operation - Type=SEQUENCE. When this parameter is
            set, the operation returns the location of the record whose sequence number matches the parameter.
        timestamp_sec (Optional) : int
            The base time for a time-based seek operation (Type=TIME), as a Unix timestamp in seconds. For example,
            1511260205 sets the search base time to 21 Nov 2017 at 10:30:05 AM UTC. The TimestampNSec request parameter
            sets the nanoseconds unit of the seek base time.

            When the TimestampSec and TimestampNSec parameters are set, the operation searches for the location of the
            earliest ingested record in the shard (the earliest record that arrived at the platform) beginning at the
            specified base time. If no matching record is found (i.e., if all records in the shard arrived before the
            specified base time), return the last location in the shard.
        timestamp_nsec (Optional) : int
            The nanoseconds unit of the TimestampSec base-time timestamp for a time-based seek operation (Type=TIME).
            For example, if TimestampSec is 1511260205 and TimestampNSec is 500000000, seek should search for the
            earliest ingested record since 21 Nov 2017 at 10:30 AM and 5.5 seconds.

        Return Value
        ----------
        A `Response` object, whose `output` is `SeekShardOutput`.
        """
        stream_path = self._ensure_path_ends_with_slash(os.path.join(stream_path, str(shard_id)))

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_seek_shard,
                                       locals(),
                                       v3io.dataplane.output.SeekShardOutput)

    def put_records(self,
                    container,
                    stream_path,
                    records,
                    access_key=None,
                    raise_for_status=None,
                    transport_actions=None):
        """Adds records to a stream.

        You can optionally assign a record to specific stream shard by specifying a related shard ID, or associate
        the record with a specific partition key to ensure that similar records are assigned to the same shard.
        By default, the platform assigns records to shards using a Round Robin algorithm. The max number of records
        is 1000.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/streaming-web-api/putrecords/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        stream_path (Required) : str
            The stream_path of the stream.
        records (Required) : []dict
            A list of dictionaries with the following keys:
            - shard_id: int, optional
                        The ID of the shard to which to assign the record, as an integer between 0 and one less than
                        the stream's shard count. When both ShardId and PartitionKey are set, the record is assigned
                        according to the shard ID, and PartitionKey is ignored. When neither a Shard ID or a partition
                        key is provided in the request, the platform's default shard-assignment algorithm is used.
            - data: str, required
                    Record data.
            - client_info: bytes/bytearray, optional
                           Custom opaque information that can optionally be provided by the producer.
                           This metadata can be used, for example, to save the data format of a record, or the time at
                           which a sensor or application event was triggered.
            - partition_key: str, optional
                             A partition key with which to associate the record (see Record Metadata). Records with the
                             same partition key are assigned to the same shard, subject to the following exceptions: if
                             a shard ID is also provided for the record (see the Records ShardId request parameter),
                             the record is assigned according to the shard ID, and PartitionKey is ignored. In addition,
                             if you increase a stream's shard count after its creation (see UpdateStream), new records
                             with a previously used partition key will be assigned either to the same shard that was
                             previously used for this partition key or to a new shard. All records with the same
                             partition key that are added to the stream after the shard-count change will be assigned
                             to the same shard (be it the previously used shard or a new shard). When neither a Shard
                             ID or a partition key is provided in the request, the platform's default shard-assignment
                             algorithm is used

            For example:
                [
                    {'shard_id': 1, 'data': 'first shard record #1'},
                    {'shard_id': 1, 'data': 'first shard record #2'},
                    {'shard_id': 10, 'data': 'invalid shard record #1'},
                    {'shard_id': 2, 'data': 'second shard record #1'},
                    {'data': 'some shard record #1'},
                ]

        Return Value
        ----------
        A `Response` object, whose `output` is `PutRecordsOutput`.
        """
        stream_path = self._ensure_path_ends_with_slash(stream_path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_put_records,
                                       locals(),
                                       v3io.dataplane.output.PutRecordsOutput)

    def get_records(self,
                    container,
                    stream_path,
                    shard_id,
                    location,
                    access_key=None,
                    raise_for_status=None,
                    transport_actions=None,
                    limit=None):
        """Retrieves (consumes) records from a stream shard.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/streaming-web-api/getrecords/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        stream_path (Required) : str
            The stream_path of the stream
        location (Required) : str
            The location within the shard at which to begin consuming records.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        limit (Optional) : int
            The maximum number of records to return in the response. The minimum is 1. There's no restriction on
            the amount of returned records, but the maximum supported overall size of all the returned records is
            10 MB and the maximum size of a single record is 2 MB, so calculate the limit accordingly.

        Return Value
        ----------
        A `Response` object, whose `output` is `GetRecordsOutput`.
        """
        stream_path = self._ensure_path_ends_with_slash(os.path.join(stream_path, str(shard_id)))

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_records,
                                       locals(),
                                       v3io.dataplane.output.GetRecordsOutput)
