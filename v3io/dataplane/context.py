import os

import future.utils

import v3io.dataplane.transport
import v3io.dataplane.session
import v3io.dataplane.request
import v3io.dataplane.response
import v3io.dataplane.output
import v3io.dataplane.items_cursor
import v3io.common.helpers


class Context(object):

    def __init__(self, logger, endpoints=None, max_connections=4, timeout=None):
        self._logger = logger
        self._transport = v3io.dataplane.transport.Transport(logger, endpoints, max_connections, timeout)
        self._access_key = os.environ['V3IO_ACCESS_KEY']

    def new_session(self, access_key=None):
        return v3io.dataplane.session.Session(self,
                                              self._transport,
                                              access_key or self._access_key)

    #
    # Container
    #

    def get_containers(self, access_key=None):
        return self._transport.encode_and_send(None,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_get_containers,
                                               locals(),
                                               v3io.dataplane.output.GetContainersOutput)

    def get_container_contents(self,
                               container_name,
                               path,
                               access_key=None,
                               get_all_attributes=None,
                               directories_only=None,
                               limit=None,
                               marker=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_get_container_contents,
                                               locals(),
                                               v3io.dataplane.output.GetContainerContentsOutput)

    #
    # Object
    #

    def get_object(self, container_name, path, access_key=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_get_object,
                                               locals())

    def put_object(self, container_name, path, access_key=None, offset=None, body=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_put_object,
                                               locals())

    def delete_object(self, container_name, path, access_key=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_delete_object,
                                               locals())

    #
    # KV
    #

    def new_items_cursor(self,
                         container_name,
                         path,
                         access_key=None,
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
        return v3io.dataplane.items_cursor.ItemsCursor(self,
                                                       container_name,
                                                       access_key or self._access_key,
                                                       path,
                                                       table_name,
                                                       attribute_names,
                                                       filter,
                                                       marker,
                                                       sharding_key,
                                                       limit,
                                                       segment,
                                                       total_segments,
                                                       sort_key_range_start,
                                                       sort_key_range_end)

    def put_item(self, container_name, path, attributes, access_key=None, condition=None, update_mode=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_put_item,
                                               locals())

    def put_items(self, container_name, path, items, access_key=None, condition=None):
        responses = v3io.dataplane.response.Responses()

        for item_path, item_attributes in future.utils.viewitems(items):

            # create a put item input
            response = self.put_item(container_name,
                                     v3io.common.helpers.url_join(path, item_path),
                                     item_attributes,
                                     condition=condition)

            # add the response
            responses.add_response(response)

        return responses

    def update_item(self, container_name, path, access_key=None, attributes=None, expression=None, condition=None,
                    update_mode=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_update_item,
                                               locals())

    def get_item(self, container_name, path, access_key=None, attribute_names='*'):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_get_item,
                                               locals(),
                                               v3io.dataplane.output.GetItemOutput)

    def get_items(self,
                  container_name,
                  path,
                  access_key=None,
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
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_get_items,
                                               locals(),
                                               v3io.dataplane.output.GetItemsOutput)

    #
    # Stream
    #

    def create_stream(self,
                      container_name,
                      path,
                      shard_count,
                      access_key=None,
                      retention_period_hours=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_create_stream,
                                               locals())

    def delete_stream(self, container_name, path, access_key=None):
        response = self.get_container_contents(path)

        # nothing to do
        if response.status_code == 404:
            return response

        for stream_shard in response.output.contents:
            self.delete_object(stream_shard.key)

        return self.delete_object(path)

    def describe_stream(self, container_name, path, access_key=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_describe_stream,
                                               locals(),
                                               v3io.dataplane.output.DescribeStreamOutput)

    def seek_shard(self, container_name, path, seek_type, access_key=None, starting_sequence_number=None, timestamp=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_seek_shard,
                                               locals(),
                                               v3io.dataplane.output.SeekShardOutput)

    def put_records(self, container_name, path, records, access_key=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_put_records,
                                               locals(),
                                               v3io.dataplane.output.PutRecordsOutput)

    def get_records(self, container_name, path, location, access_key=None, limit=None):
        return self._transport.encode_and_send(container_name,
                                               access_key or self._access_key,
                                               v3io.dataplane.request.encode_get_records,
                                               locals(),
                                               v3io.dataplane.output.GetRecordsOutput)
