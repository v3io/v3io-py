import os

import v3io.dataplane.request
import v3io.dataplane.output
import v3io.dataplane.model
import v3io.aio.dataplane.kv_cursor


class Model(v3io.dataplane.model.Model):

    def __init__(self, client):
        self._client = client
        self._access_key = client._access_key
        self._transport = client._transport

    def new_cursor(self,
                   container,
                   table_path,
                   access_key=None,
                   raise_for_status=None,
                   attribute_names='*',
                   filter_expression=None,
                   marker=None,
                   sharding_key=None,
                   limit=None,
                   segment=None,
                   total_segments=None,
                   sort_key_range_start=None,
                   sort_key_range_end=None):
        return v3io.aio.dataplane.kv_cursor.Cursor(self._client,
                                                   container,
                                                   access_key or self._access_key,
                                                   table_path,
                                                   raise_for_status,
                                                   attribute_names,
                                                   filter_expression,
                                                   marker,
                                                   sharding_key,
                                                   limit,
                                                   segment,
                                                   total_segments,
                                                   sort_key_range_start,
                                                   sort_key_range_end)

    async def put(self,
                  container,
                  table_path,
                  key,
                  attributes,
                  access_key=None,
                  raise_for_status=None,
                  condition=None):
        """Creates an item with the provided attributes. If an item with the same name (primary key) already exists in
        the specified table, the existing item is completely overwritten (replaced with a new item). If the item or
        table do not exist, the operation creates them.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/nosql-web-api/putitem/.

        Notes:
        1. To provide arrays, pass either a list of integers ([1, 2, 3]), a list of floats ([1.0, 2.0, 3.0]) an
           array.array with a typecode of either 'l' (integer) or 'd' (float). The response will always either be
           a list of integers or a list of floats (never an array.array)
        2. To provide a timestamp, pass a datetime.datetime. Whatever the timezone, it will be stored as UTC and
           a UTC datetime will be retreived when read

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        table_path (Required) : str
            The full path of the table
        key (Required) : str
            The item key name
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

        Return Value
        ----------
        A `Response` object.
        """

        return await self._transport.request(container,
                                             access_key or self._access_key,
                                             raise_for_status,
                                             v3io.dataplane.request.encode_put_item,
                                             locals())

    async def update(self,
                     container,
                     table_path,
                     key,
                     access_key=None,
                     raise_for_status=None,
                     attributes=None,
                     expression=None,
                     condition=None,
                     update_mode=None,
                     alternate_expression=None):
        """Updates the attributes of a table item. If the specified item or table don't exist,
        the operation creates them.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/nosql-web-api/updateitem/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        table_path (Required) : str
            The full path of the table
        key (Required) : str
            The item key name
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
            See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/nosql-web-api/putitem/.
        update_mode (Optional) : str
            CreateOrReplaceAttributes (default): Creates or replaces attributes
        alternate_expression (Optional) : str
            An alternate update expression that specifies the changes to make to the item's attributes when a
            condition expression, defined in the ConditionExpression request parameter, evaluates to false;
            (i.e., this parameter defines the else clause of a conditional if-then-else update expression).
            See Update Expression for syntax details and examples. When the alternate update expression is executed,
            it's evaluated against the table item to be updated, if it exists. If the item doesn't exist, the update
            creates it (as well as the parent table if it doesn't exist). See also the UpdateExpression notes, which
            apply to the alternate update expression as well.

        Return Value
        ----------
        A `Responses` object.
        """
        return await self._transport.request(container,
                                             access_key or self._access_key,
                                             raise_for_status,
                                             v3io.dataplane.request.encode_update_item,
                                             locals())

    async def get(self,
                  container,
                  table_path,
                  key,
                  access_key=None,
                  raise_for_status=None,
                  attribute_names='*'):
        """Retrieves the requested attributes of a table item.

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/nosql-web-api/getitem/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        table_path (Required) : str
            The full path of the table
        key (Required) : str
            The item key name
        attribute_names (Required) : []str or '*'
            A list of attribute names to get, or '*' which will retreive all attributes
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object, whose `output` is `GetItemOutput`.
        """
        return await self._transport.request(container,
                                             access_key or self._access_key,
                                             raise_for_status,
                                             v3io.dataplane.request.encode_get_item,
                                             locals(),
                                             v3io.dataplane.output.GetItemOutput)

    async def scan(self,
                   container,
                   table_path,
                   access_key=None,
                   raise_for_status=None,
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

        See https://www.iguazio.com/docs/latest-release/data-layer/reference/web-apis/nosql-web-api/getitems/.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        table_path (Required) : str
            The full path of the table
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        attribute_names (Optional) : []str or '*'
            A list of attribute names to get, or '*' which will retreive all attributes
        filter_expression (Optional) : str
            A filter expression that restricts the items to retrieve. Only items that match the filter criteria
            are returned.
            See https://www.iguazio.com/docs/latest-release/data-layer/reference/expressions/condition-expression/#filter-expression.
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
        table_path = self._ensure_path_ends_with_slash(table_path)

        return await self._transport.request(container,
                                             access_key or self._access_key,
                                             raise_for_status,
                                             v3io.dataplane.request.encode_get_items,
                                             locals(),
                                             v3io.dataplane.output.GetItemsOutput)

    async def delete(self, container, table_path, key, access_key=None, raise_for_status=None, transport_actions=None):
        """Deletes an item.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        table_path (Required) : str
            The full path of the table
        key (Required) : str
            The item key name
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object.
        """
        return self._client.delete_object(container,
                                          os.path.join(table_path, key),
                                          access_key,
                                          raise_for_status,
                                          transport_actions)

    async def create_schema(self,
                            container,
                            table_path,
                            access_key=None,
                            raise_for_status=None,
                            key=None,
                            fields=None):
        """Creates a KV schema file

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        table_path (Required) : str
            The full path of the table
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
        put_object_args['path'] = os.path.join(put_object_args['table_path'], '.#schema')
        put_object_args['offset'] = 0
        put_object_args['append'] = None
        put_object_args['body'] = self._client._get_schema_contents(key, fields)
        del (put_object_args['key'])
        del (put_object_args['fields'])

        return await self._transport.request(container,
                                             access_key or self._access_key,
                                             raise_for_status,
                                             v3io.dataplane.request.encode_put_object,
                                             put_object_args)
