import future.utils

import v3io.dataplane.request
import v3io.dataplane.response
import v3io.dataplane.output
import v3io.common.helpers
import v3io.dataplane.items_cursor


class Container(object):

    def __init__(self, access_key, transport, container_name):
        self._access_key = access_key
        self._transport = transport
        self._container_name = container_name

    #
    # Container
    #

    def get_container_contents(self,
                               path,
                               get_all_attributes=None,
                               directories_only=None,
                               limit=None,
                               marker=None):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_get_container_contents,
                                               locals(),
                                               v3io.dataplane.output.GetContainerContentsOutput)

    #
    # Object
    #

    def get_object(self, path):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_get_object,
                                               locals())

    def put_object(self, path, offset=None, body=None):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_put_object,
                                               locals())

    def delete_object(self, path):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_delete_object,
                                               locals())

    #
    # KV
    #

    def new_items_cursor(self,
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
        return v3io.dataplane.items_cursor.ItemsCursor(self,
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

    def put_item(self, path, attributes, condition=None, update_mode=None):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_put_item,
                                               locals())

    def put_items(self, path, items, condition=None):
        responses = v3io.dataplane.response.Responses()

        for item_path, item_attributes in future.utils.viewitems(items):
            # create a put item input
            response = self.put_item(v3io.common.helpers.url_join(path, item_path),
                                     item_attributes,
                                     condition=condition)

            # add the response
            responses.add_response(response)

        return responses

    def update_item(self, path, attributes=None, expression=None, condition=None, update_mode=None):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_update_item,
                                               locals())

    def get_item(self, path, attribute_names='*'):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_get_item,
                                               locals(),
                                               v3io.dataplane.output.GetItemOutput)

    def get_items(self,
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
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_get_items,
                                               locals(),
                                               v3io.dataplane.output.GetItemsOutput)

    #
    # Stream
    #

    def create_stream(self,
                      path,
                      shard_count,
                      retention_period_hours=None):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_create_stream,
                                               locals())

    def delete_stream(self, path):
        response = self.get_container_contents(path)

        # nothing to do
        if response.status_code == 404:
            return response

        for stream_shard in response.output.contents:
            self.delete_object(stream_shard.key)

        return self.delete_object(path)

    def describe_stream(self, path):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_describe_stream,
                                               locals(),
                                               v3io.dataplane.output.DescribeStreamOutput)

    def seek_shard(self, path, seek_type, starting_sequence_number=None, timestamp=None):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_seek_shard,
                                               locals(),
                                               v3io.dataplane.output.SeekShardOutput)

    def put_records(self, path, records):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_put_records,
                                               locals(),
                                               v3io.dataplane.output.PutRecordsOutput)

    def get_records(self, path, location, limit=None):
        return self._transport.encode_and_send(self._container_name,
                                               self._access_key,
                                               v3io.dataplane.request.encode_get_records,
                                               locals(),
                                               v3io.dataplane.output.GetRecordsOutput)
