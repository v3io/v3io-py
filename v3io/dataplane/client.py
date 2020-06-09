import os
import ujson

import future.utils

import v3io.dataplane.transport.requests
import v3io.dataplane.transport.httpclient
import v3io.dataplane.request
import v3io.dataplane.batch
import v3io.dataplane.response
import v3io.dataplane.output
import v3io.dataplane.items_cursor
import v3io.common.helpers
import v3io.logger


class Client(object):

    def __init__(self,
                 logger=None,
                 endpoint=None,
                 access_key=None,
                 max_connections=None,
                 timeout=None,
                 transport_kind='httpclient'):
        """Creates a v3io client, used to access v3io

        Parameters
        ----------
        logger (Optional) : logger
            On optional pre-existing logger. If not passed, a logger is created with "INFO" level
        endpoint (Optional) : str
            The v3io endpoint to connect to (e.g. http://v3io-webapi:8081). if empty, the env var
            V3IO_API is used
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env. this can
            be overridden per request if needed
        max_connections (Optional) : int
            The number of connections to create towards v3io - defining the max number of parallel
            operations towards v3io. Defaults to 8
        timeout (Optional) : None
            For future use
        transport_kind (Optional) : str
            Defines the underlying transport to use towards v3io (of of httpclient, requests). Should
            normally be left httpclient unless an underlying issue is found, in which case requests may
            be used as a temporary workaround at the expense of performance

        Return Value
        ----------
        A `Client` object
        """
        self._logger = logger or v3io.logger.Logger(level='INFO')
        self._access_key = access_key or os.environ.get('V3IO_ACCESS_KEY')

        if not self._access_key:
            raise ValueError('Access key must be provided in Client() arguments or in the '
                             'V3IO_ACCESS_KEY environment variable')

        # get the transport class
        transport_cls = getattr(v3io.dataplane.transport, transport_kind)

        self._transport = transport_cls.Transport(self._logger,
                                                  endpoint,
                                                  max_connections,
                                                  timeout)

        # create a default "batch" object
        self.batch = self.create_batch()

    def create_batch(self):
        return v3io.dataplane.batch.Batch(self)

    def close(self):
        self._transport.close()

    #
    # Container
    #

    def get_containers(self, access_key=None, raise_for_status=None, transport_actions=None):
        """Lists the containers that are visible to the user who sent the request, according to its tenant.

        Parameters
        ----------
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object, whose `output` is `GetContainersOutput`.
        """

        return self._transport.request(None,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_containers,
                                       locals(),
                                       v3io.dataplane.output.GetContainersOutput)

    def get_container_contents(self,
                               container,
                               path,
                               access_key=None,
                               raise_for_status=None,
                               transport_actions=None,
                               get_all_attributes=None,
                               directories_only=None,
                               limit=None,
                               marker=None):
        """Lists the containers contents.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path within the container
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        get_all_attributes (Optional) : bool
            False (default) - retrieves basic attributes
            True - retrieves all attributes of the underlying objects
        directories_only (Optional) : bool
            False (default) - retrieves objects (contents) and directories (common prefixes)
            True - retrieves only directories (common prefixes)
        limit (Optional) : int
            Number of objects/directories to receive. default: 1000
        marker (Optional) : str
            An opaque identifier that was returned in the NextMarker element of a response to a previous
            get_container_contents request that did not return all the requested items. This marker identifies the
            location in the path from which to start searching for the remaining requested items.

        Return Value
        ----------
        A `Response` object, whose `output` is `GetContainerContentsOutput`.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_container_contents,
                                       locals(),
                                       v3io.dataplane.output.GetContainerContentsOutput)

    #
    # Object
    #

    def get_object(self,
                   container,
                   path,
                   access_key=None,
                   raise_for_status=None,
                   transport_actions=None,
                   offset=None,
                   num_bytes=None):
        """Retrieves an object from a container.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        offset (Optional) : int
            A numeric offset into the object (in bytes). Defaults to 0
        num_bytes (Optional) : int
            Number of bytes to return. By default equal to len(object)-offset

        Return Value
        ----------
        A `Response` object, whose `body` is populated with the body of the object.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_object,
                                       locals())

    def put_object(self,
                   container,
                   path,
                   access_key=None,
                   raise_for_status=None,
                   transport_actions=None,
                   body=None,
                   append=None):
        """Adds a new object to a container, or appends data to an existing object. The option to append data is
        extension to the S3 PUT Object capabilities

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        body (Optional) : str
            The contents of the object
        append (Optional) : bool
            If True, the put appends the data to the end of the object. Defaults to False

        Return Value
        ----------
        A `Response` object
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_put_object,
                                       locals())

    def delete_object(self, container, path, access_key=None, raise_for_status=None, transport_actions=None):
        """Deletes an object from a container.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_delete_object,
                                       locals())

    #
    # KV
    #

    def new_items_cursor(self,
                         container,
                         path,
                         access_key=None,
                         raise_for_status=None,
                         table_name=None,
                         attribute_names='*',
                         filter_expression=None,
                         marker=None,
                         sharding_key=None,
                         limit=None,
                         segment=None,
                         total_segments=None,
                         sort_key_range_start=None,
                         sort_key_range_end=None):
        return v3io.dataplane.items_cursor.ItemsCursor(self,
                                                       container,
                                                       access_key or self._access_key,
                                                       path,
                                                       raise_for_status,
                                                       table_name,
                                                       attribute_names,
                                                       filter_expression,
                                                       marker,
                                                       sharding_key,
                                                       limit,
                                                       segment,
                                                       total_segments,
                                                       sort_key_range_start,
                                                       sort_key_range_end)

    def put_item(self,
                 container,
                 path,
                 attributes,
                 access_key=None,
                 raise_for_status=None,
                 transport_actions=None,
                 condition=None,
                 update_mode=None):
        """Creates an item with the provided attributes. If an item with the same name (primary key) already exists in
        the specified table, the existing item is completely overwritten (replaced with a new item). If the item or
        table do not exist, the operation creates them.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/nosql-web-api/putitem/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path and collection (table) name of the item
        attributes (Required) : dict
            The item to add - an object containing zero or more attributes.
            For example:
                {
                    'age': 42,
                    'feature': 'mustache'
                }

        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        condition (Optional) : str
            A Boolean condition expression that defines a conditional logic for executing the put-item operation.
        update_mode (Optional) : str
            CreateOrReplaceAttributes (default): Creates or replaces attributes

        Return Value
        ----------
        A `Response` object.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_put_item,
                                       locals())

    def put_items(self, container, path, items, access_key=None, raise_for_status=None, condition=None):
        """[OBSOLETED] A helper to put several items, calling put_item for each.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the item, to which the item key is concatenated.
        items (Required) : dict
            A dictionary whose keys are the item keys and values are the attributes.
            For example:
                {
                    'bob': {'age': 42, 'feature': 'mustache'},
                    'linda': {'age': 40, 'feature': 'singing'}
                }
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        condition (Optional) : str
            A Boolean condition expression that defines a conditional logic for executing the put-item operation.

        Return Value
        ----------
        A `Responses` object, holding the responses received from the individual put_item.
        """
        responses = v3io.dataplane.response.Responses()

        for item_path, item_attributes in future.utils.viewitems(items):
            # create a put item input
            response = self.put_item(container,
                                     v3io.common.helpers.url_join(path, item_path),
                                     item_attributes,
                                     access_key=access_key,
                                     condition=condition,
                                     raise_for_status=raise_for_status)

            # add the response
            responses.add_response(response)

        return responses

    def update_item(self,
                    container,
                    path,
                    access_key=None,
                    raise_for_status=None,
                    transport_actions=None,
                    attributes=None,
                    expression=None,
                    condition=None,
                    update_mode=None):
        """Updates the attributes of a table item. If the specified item or table don't exist,
        the operation creates them.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/nosql-web-api/updateitem/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path and collection (table) name of the item.
        attributes (Required) : dict
            The item to update - an object containing zero or more attributes.
            For example:
                {
                    'age': 42,
                    'feature': 'mustache'
                }
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        expression (Optional) : str
            An update expression that specifies the changes to make to the item's attributes.
        condition (Optional) : str
            A Boolean condition expression that defines a conditional logic for executing the put-item operation.
            See https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/nosql-web-api/putitem/
        update_mode (Optional) : str
            CreateOrReplaceAttributes (default): Creates or replaces attributes

        Return Value
        ----------
        A `Responses` object.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_update_item,
                                       locals())

    def get_item(self,
                 container,
                 path,
                 access_key=None,
                 raise_for_status=None,
                 transport_actions=None,
                 attribute_names='*'):
        """Retrieves the requested attributes of a table item.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/nosql-web-api/getitem/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path and collection (table) name of the item.
        attribute_names (Required) : []str or '*'
            A list of attribute names to get, or '*' which will retreive all attributes
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object, whose `output` is `GetItemOutput`.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_item,
                                       locals(),
                                       v3io.dataplane.output.GetItemOutput)

    def get_items(self,
                  container,
                  path,
                  access_key=None,
                  raise_for_status=None,
                  transport_actions=None,
                  table_name=None,
                  attribute_names='*',
                  filter_expression=None,
                  marker=None,
                  sharding_key=None,
                  limit=None,
                  segment=None,
                  total_segments=None,
                  sort_key_range_start=None,
                  sort_key_range_end=None):
        """Retrieves (reads) attributes of multiple items in a table or in a data container's root directory,
        according to the specified criteria.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/nosql-web-api/getitems/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path and collection (table) name of the item.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        table_name (Optional) : str
            If passed, appended to the path to form the collection path
        attribute_names (Optional) : []str or '*'
            A list of attribute names to get, or '*' which will retreive all attributes
        filter_expression (Optional) : str
            A filter expression that restricts the items to retrieve. Only items that match the filter criteria
            are returned. See https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/#filter-expression.md
        marker (Optional) : str
            An opaque identifier that was returned in the NextMarker element of a response to a previous GetItems
            request that did not return all the requested items. This marker identifies the location in the table
            from which to start searching for the remaining requested items. See Partial Response and the description
            of the NextMarker response element.
        sharding_key (Optional) : str
            The maximal sorting-key value of the items to get by using a range scan. The sorting-key value is the
            part to the right of the leftmost period in a compound primary-key value (item name). This parameter is
            applicable only together with the ShardingKey request parameter. The scan will return all items with the
            specified sharding-key value whose sorting-key values are greater than or equal to (>=) than the value of
            the SortKeyRangeStart parameter (if set) and less than (<) the value of the SortKeyRangeEnd parameter.
        limit (Optional) : int
            The maximum number of items to return within the response (i.e., the maximum number of elements in the
            response object's Items array).
        segment (Optional) : str
            The ID of a specific table segment to scan - 0 to one less than TotalSegment
        total_segments (Optional) : str
            The number of segments into which to divide the table scan - 1 to 1024. See Parallel Scan.
            The segments are assigned sequential IDs starting with 0.
        sort_key_range_start (Optional) : str
            The minimal sorting-key value of the items to get by using a range scan. The sorting-key value is the part
            to the right of the leftmost period in a compound primary-key value (item name). This parameter is
            applicable only together with the ShardingKey request parameter. The scan will return all items with
            the specified sharding-key value whose sorting-key values are greater than or equal to (>=) the value of
            the SortKeyRangeStart parameter and less than (<) the value of the SortKeyRangeEnd parameter (if set).
        sort_key_range_end (Optional) : str
            The maximal sorting-key value of the items to get by using a range scan. The sorting-key value is the part
             to the right of the leftmost period in a compound primary-key value (item name). This parameter is
             applicable only together with the ShardingKey request parameter. The scan will return all items with
             the specified sharding-key value whose sorting-key values are greater than or equal to (>=) than the
             value of the SortKeyRangeStart parameter (if set) and less than (<) the value of the SortKeyRangeEnd
             parameter.

        Return Value
        ----------
        A `Response` object, whose `output` is `GetItemsOutput`.
        """
        path = self._ensure_path_ends_with_slash(path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_items,
                                       locals(),
                                       v3io.dataplane.output.GetItemsOutput)

    #
    # Stream
    #

    def create_stream(self,
                      container,
                      path,
                      shard_count,
                      access_key=None,
                      raise_for_status=None,
                      transport_actions=None,
                      retention_period_hours=None):
        """Creates and configures a new stream. The configuration includes the stream's shard count and retention
        period. The new stream is available immediately upon its creation.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/streaming-web-api/createstream/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
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
        path = self._ensure_path_ends_with_slash(path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_create_stream,
                                       locals())

    def delete_stream(self, container, path, access_key=None, raise_for_status=None):
        """Deletes a stream object along with all of its shards.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the stream.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object.
        """
        path = self._ensure_path_ends_with_slash(path)

        response = self.get_container_contents(container,
                                               path,
                                               access_key,
                                               raise_for_status)

        # nothing to do
        if response.status_code == 404:
            return response

        for stream_shard in response.output.contents:
            self.delete_object(container, stream_shard.key, access_key, raise_for_status)

        return self.delete_object(container, path, access_key, raise_for_status)

    def describe_stream(self, container, path, access_key=None, raise_for_status=None, transport_actions=None):
        """Retrieves a stream's configuration, including the shard count and retention period.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/streaming-web-api/describestream/

        Parameters
        ----------
        path (Required) : str
            The path of the stream.
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object, whose `output` is `DescribeStreamOutput`.
        """
        path = self._ensure_path_ends_with_slash(path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_describe_stream,
                                       locals(),
                                       v3io.dataplane.output.DescribeStreamOutput)

    def seek_shard(self,
                   container,
                   path,
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

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/streaming-web-api/seek/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the stream.
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
        path = self._ensure_path_ends_with_slash(path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_seek_shard,
                                       locals(),
                                       v3io.dataplane.output.SeekShardOutput)

    def put_records(self, container, path, records, access_key=None, raise_for_status=None, transport_actions=None):
        """Adds records to a stream.

        You can optionally assign a record to specific stream shard by specifying a related shard ID, or associate
        the record with a specific partition key to ensure that similar records are assigned to the same shard.
        By default, the platform assigns records to shards using a Round Robin algorithm.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/streaming-web-api/putrecords/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the stream.
        records (Required) : []dict
            A list of dictionaries with the following keys:
            - shard_id: The ID of the shard to which to assign the record, as an integer between 0 and one less than
                        the stream's shard count. When both ShardId and PartitionKey are set, the record is assigned
                        according to the shard ID, and PartitionKey is ignored. When neither a Shard ID or a partition
                        key is provided in the request, the platform's default shard-assignment algorithm is used.
            - data: Record data.
            - client_info: Custom opaque information that can optionally be provided by the producer. This metadata can
                           be used, for example, to save the data format of a record, or the time at which a sensor or
                           application event was triggered.
            - partition_key: A partition key with which to associate the record (see Record Metadata). Records with the
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
        path = self._ensure_path_ends_with_slash(path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_put_records,
                                       locals(),
                                       v3io.dataplane.output.PutRecordsOutput)

    def get_records(self,
                    container,
                    path,
                    location,
                    access_key=None,
                    raise_for_status=None,
                    transport_actions=None,
                    limit=None):
        """Retrieves (consumes) records from a stream shard.

        See:
        https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/streaming-web-api/getrecords/

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the stream, whose last element is the shard id (e.g. /my-stream/0)
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
        path = self._ensure_path_ends_with_slash(path)

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_records,
                                       locals(),
                                       v3io.dataplane.output.GetRecordsOutput)

    #
    # Helpers
    #

    def create_schema(self,
                      container,
                      path,
                      access_key=None,
                      raise_for_status=None,
                      transport_actions=None,
                      key=None,
                      fields=None):
        """Creates a KV schema file

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        key (Required) : str
            The key field name
        fields (Required) : list of dicts
            A dictionary of fields, where each item has:
            - name (string)
            - type (string - one of string, double, long)
            - nullable (boolean)

            Example: [
                {
                  'name': 'my_field',
                  'type': 'string',
                  'nullable': False
                },
                {
                  'name': 'who',
                  'type': 'string',
                  "nullable": True
                }
            ]

        Return Value
        ----------
        A `Response` object
        """
        put_object_args = locals()
        put_object_args['path'] = os.path.join(put_object_args['path'], '.%23schema')
        put_object_args['offset'] = 0
        put_object_args['append'] = None
        put_object_args['body'] = self._get_schema_contents(key, fields)
        del(put_object_args['key'])
        del (put_object_args['fields'])

        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_put_object,
                                       put_object_args)

    @staticmethod
    def _ensure_path_ends_with_slash(path):
        if not path.endswith('/'):
            return path + '/'

        return path

    @staticmethod
    def _get_schema_contents(key, fields):
        return ujson.dumps({
            'hashingBucketNum': 0,
            'key': key,
            'fields': fields
        })
