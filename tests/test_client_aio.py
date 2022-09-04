# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import unittest
import os
import array
import datetime
import future.utils

import v3io.dataplane
import v3io.aio.dataplane


class Test(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._client = v3io.aio.dataplane.Client(logger_verbosity='DEBUG',
                                                 transport_verbosity='DEBUG')

        self._container = 'bigdata'
        self._test_parent_dir = os.environ.get("MATRIX_PYTHON_VERSION")
        if self._test_parent_dir is None:
            self._test_parent_dir = ''

    async def asyncTearDown(self):
        await self._client.close()

    async def _delete_dir(self, path):
        response = await self._client.container.list(container=self._container,
                                                     path=path,
                                                     raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)

        if response.status_code == 404:
            return

        if response.status_code != 200:
            raise RuntimeError(response.body)

        for content in response.output.contents:
            await self._client.object.delete(container=self._container, path=content.key)

        for common_prefixes in response.output.common_prefixes:
            await self._client.object.delete(container=self._container,
                                             path=common_prefixes.prefix)


class TestContainer(Test):

    async def asyncSetUp(self):
        await super(TestContainer, self).asyncSetUp()
        self._path = os.path.join(self._test_parent_dir, 'v3io-py-test-container')

        # clean up
        await self._delete_dir(self._path)

    async def test_get_container_contents_invalid_path(self):
        response = await self._client.container.list(container=self._container,
                                                     path='/no-such-path',
                                                     raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        self.assertEqual(404, response.status_code)
        self.assertIn('No such file', str(response.body))

    async def test_get_container_contents(self):
        body = 'If you cannot do great things, do small things in a great way.'

        for object_index in range(5):
            await self._client.object.put(container=self._container,
                                          path=os.path.join(self._path, 'object-{0}.txt'.format(object_index)),
                                          body=body)

        for object_index in range(5):
            await self._client.object.put(container=self._container,
                                          path=os.path.join(self._path, 'dir-{0}/'.format(object_index)))

        response = await self._client.container.list(container=self._container,
                                                     path=self._path,
                                                     get_all_attributes=True,
                                                     directories_only=True)
        self.assertEqual(0, len(response.output.contents))
        self.assertNotEqual(0, len(response.output.common_prefixes))

        response = await self._client.container.list(container=self._container,
                                                     path=self._path,
                                                     get_all_attributes=True)
        self.assertNotEqual(0, len(response.output.contents))
        self.assertNotEqual(0, len(response.output.common_prefixes))

        # clean up
        await self._delete_dir(self._path)


class TestStream(Test):

    async def asyncSetUp(self):
        await super(TestStream, self).asyncSetUp()

        self._path = os.path.join(self._test_parent_dir, 'v3io-py-test-stream')

        # clean up
        await self._client.stream.delete(container=self._container,
                                         stream_path=self._path,
                                         raise_for_status=[200, 204, 404])

    async def test_delete_stream_with_cg(self):
        num_shards = 8

        # check that the stream doesn't exist
        self.assertFalse(await self._stream_exists())

        # create a stream
        await self._client.stream.create(container=self._container,
                                         stream_path=self._path,
                                         shard_count=num_shards)

        # write data to all shards so there are files
        for shard_id in range(num_shards):
            await self._client.stream.put_records(container=self._container,
                                                  stream_path=self._path,
                                                  records=[
                                                      {'shard_id': shard_id,
                                                       'data': 'data for shard {}'.format(shard_id)}
                                                  ])

        # write several "consumer group state" files
        for cg_id in range(3):
            await self._client.object.put(container=self._container,
                                          path=os.path.join(self._path, 'cg{}-state.json'.format(cg_id)))

        # check that the stream doesn't exist
        self.assertTrue(await self._stream_exists())

        # delete the stream
        await self._client.stream.delete(container=self._container, stream_path=self._path)

        # check that the stream doesn't exist
        self.assertFalse(await self._stream_exists())

    async def test_stream(self):

        # create a stream w/8 shards
        await self._client.stream.create(container=self._container,
                                         stream_path=self._path,
                                         shard_count=8)

        records = [
            {'shard_id': 1, 'data': 'first shard record #1'},
            {'shard_id': 1, 'data': 'first shard record #2', 'client_info': bytearray(b'some info')},
            {'shard_id': 10, 'data': 'invalid shard record #1'},
            {'shard_id': 2, 'data': 'second shard record #1'},
            {'data': 'some shard record #1'},
        ]

        response = await self._client.stream.put_records(container=self._container,
                                                         stream_path=self._path,
                                                         records=records)
        self.assertEqual(1, response.output.failed_record_count)

        for response_record_index, response_record in enumerate(response.output.records):
            if response_record_index == 2:
                self.assertIsNotNone(response_record.error_code)
            else:
                self.assertIsNone(response_record.error_code)

        response = await self._client.stream.seek(container=self._container,
                                                  stream_path=self._path,
                                                  shard_id=1,
                                                  seek_type='EARLIEST')

        self.assertNotEqual('', response.output.location)

        response = await self._client.stream.get_records(container=self._container,
                                                         stream_path=self._path,
                                                         shard_id=1,
                                                         location=response.output.location)

        self.assertEqual(2, len(response.output.records))
        self.assertEqual(records[0]['data'], response.output.records[0].data.decode('utf-8'))
        self.assertEqual(records[1]['data'], response.output.records[1].data.decode('utf-8'))
        self.assertEqual(records[1]['client_info'], response.output.records[1].client_info)

        # update the stream by adding 8 shards to it
        await self._client.stream.update(container=self._container,
                                         stream_path=self._path,
                                         shard_count=16)

        records = [
            {'shard_id': 10, 'data': 'Now valid shard record #1'},
        ]

        response = await self._client.stream.put_records(container=self._container,
                                                         stream_path=self._path,
                                                         records=records)

        self.assertEqual(0, response.output.failed_record_count)

        await self._client.stream.delete(container=self._container,
                                         stream_path=self._path)

    async def _stream_exists(self):
        response = await self._client.stream.describe(container=self._container,
                                                      stream_path=self._path,
                                                      raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
        return response.status_code == 200


class TestObject(Test):

    async def asyncSetUp(self):
        await super(TestObject, self).asyncSetUp()

        self._object_dir = os.path.join(self._test_parent_dir, 'v3io-py-test-object')
        self._object_path = self._object_dir + '/obj ect.txt'

        # clean up
        await self._delete_dir(self._object_dir)

    async def test_object(self):
        contents = 'vegans are better than everyone'

        response = await self._client.object.get(container=self._container,
                                                 path=self._object_path,
                                                 raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)

        self.assertEqual(404, response.status_code)

        # put contents to some object
        await self._client.object.put(container=self._container,
                                      path=self._object_path,
                                      body=contents)

        # get the contents
        response = await self._client.object.get(container=self._container,
                                                 path=self._object_path)

        if not isinstance(response.body, str):
            response.body = response.body.decode('utf-8')

        self.assertEqual(response.body, contents)

        # get the head of the object
        response = await self._client.object.head(container=self._container,
                                           path=self._object_path)

        self.assertIn(('Content-Length', str(len(contents))), response.headers.items())

        # get the head of the dir-object
        response = await self._client.object.head(container=self._container,
                                           path=self._object_dir)

        self.assertIn(('Content-Length', str(0)), response.headers.items())

        # delete the object
        await self._client.object.delete(container=self._container,
                                         path=self._object_path)

        # get again
        response = await self._client.object.get(container=self._container,
                                                 path=self._object_path,
                                                 raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)

        self.assertEqual(404, response.status_code)

    async def test_append(self):
        contents = [
            'First part',
            'Second part',
            'Third part',
        ]

        # put the contents into the object
        for content in contents:
            await self._client.object.put(container=self._container,
                                          path=self._object_path,
                                          body=content,
                                          append=True)

        # get the contents
        response = await self._client.object.get(container=self._container,
                                                 path=self._object_path)

        self.assertEqual(response.body.decode('utf-8'), ''.join(contents))

    async def test_get_offset(self):
        await self._client.object.put(container=self._container,
                                      path=self._object_path,
                                      body='1234567890')

        # get the contents without limit
        response = await self._client.object.get(container=self._container,
                                                 path=self._object_path,
                                                 offset=4)

        self.assertEqual(response.body.decode('utf-8'), '567890')

        # get the contents with limit
        response = await self._client.object.get(container=self._container,
                                                 path=self._object_path,
                                                 offset=4,
                                                 num_bytes=3)

        self.assertEqual(response.body.decode('utf-8'), '567')


# class TestSchema(Test):
#
#     async def asyncSetUp(self):
#         await super(TestSchema, self).asyncSetUp()
#
#         self._schema_dir = '/v3io-py-test-schemaa'
#         self._schema_path = os.path.join(self._schema_dir, '.%23schema')
#
#         # clean up
#         await self._delete_dir(self._schema_dir)
#
#     async def test_create_schema(self):
#         await self._client.kv.create_schema(container=self._container,
#                                             table_path=self._schema_dir,
#                                             key='key_field',
#                                             fields=[
#                                                 {
#                                                     'name': 'key_field',
#                                                     'type': 'string',
#                                                     'nullable': False
#                                                 },
#                                                 {
#                                                     'name': 'data_field_0',
#                                                     'type': 'long',
#                                                     'nullable': True
#                                                 },
#                                                 {
#                                                     'name': 'data_field_1',
#                                                     'type': 'double',
#                                                     'nullable': True
#                                                 },
#                                             ])
#
#         # write to test the values in the UI (requires breaking afterwards)
#         items = {
#             'a': {'data_field_0': 30, 'data_field_1': 100},
#             'b': {'data_field_0': 300, 'data_field_1': 1000},
#             'c': {'data_field_0': 3000, 'data_field_1': 10000},
#         }
#
#         for item_key, item_attributes in future.utils.viewitems(items):
#             await self._client.kv.put(container=self._container,
#                                       table_path=self._schema_dir,
#                                       key=item_key,
#                                       attributes=item_attributes)
#
#         # verify the scehma
#         response = await self._client.object.get(container=self._container,
#                                                  path=self._schema_path,
#                                                  raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)
#
#         # find a way to assert this without assuming serialization order
#         # self.assertEqual(
#         #     '{"hashingBucketNum":0,"key":"key_field","fields":[{"name":"key_field","type":"string","nullable":false},'
#         #     '{"name":"data_field_0","type":"long","nullable":true},{"name":"data_field_1","type":"double"'
#         #     ',"nullable":true}]}',
#         #     response.body.decode('utf-8'))


class TestKv(Test):

    async def asyncSetUp(self):
        await super(TestKv, self).asyncSetUp()

        self._path = os.path.join(self._test_parent_dir, 'some_dir/v3io-py-test-emd')
        await self._delete_dir(self._path)

    async def test_kv_array(self):
        item_key = 'item_with_arrays'
        item = {
            'list_with_ints': [1, 2, 3],
            'list_with_floats': [10.25, 20.25, 30.25],
        }

        await self._client.kv.put(container=self._container,
                                  table_path=self._path,
                                  key=item_key,
                                  attributes=item)

        for attribute_name in item.keys():
            await self._client.kv.update(container=self._container,
                                         table_path=self._path,
                                         key=item_key,
                                         expression=f'{attribute_name}[1]={attribute_name}[1]*2')

        # get the item
        response = await self._client.kv.get(container=self._container,
                                             table_path=self._path,
                                             key=item_key)

        for attribute_name in item.keys():
            self.assertEqual(response.output.item[attribute_name][1], item[attribute_name][1] * 2)

    async def test_kv_values(self):

        def _get_int_array():
            int_array = array.array('l')
            for value in range(10):
                int_array.append(value)

            return int_array

        def _get_float_array():
            float_array = array.array('d')
            for value in range(10):
                float_array.append(value)

            return float_array

        item_key = 'bob'
        item = {
            item_key: {
                'age': 42,
                'pi': 3.14,
                'feature_str': 'mustache',
                'feature_unicode': u'mustache',
                'numeric_str': '1',
                'unicode': u'\xd7\xa9\xd7\x9c\xd7\x95\xd7\x9d',
                'male': True,
                'happy': False,
                'blob': b'+AFymWFzAL/LUOiU2huiADbugMH0AARATEO1',
                'list_with_ints': [1, 2, 3],
                'list_with_floats': [10.5, 20.5, 30.5],
                'array_with_ints': _get_int_array(),
                'array_with_floats': _get_float_array(),
                'now': datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
            }
        }

        await self._client.kv.put(container=self._container,
                                  table_path=self._path,
                                  key=item_key,
                                  attributes=item[item_key])

        response = await self._client.kv.get(container=self._container,
                                             table_path=self._path,
                                             key=item_key)

        self.assertEqual(len(item[item_key].keys()), len(response.output.item.keys()))

        for key, value in response.output.item.items():
            self._compare_item_values(item[item_key][key], value)

        for key, value in item[item_key].items():
            self._compare_item_types(item[item_key][key], response.output.item[key])

    async def test_kv(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        for item_key, item_attributes in future.utils.viewitems(items):
            await self._client.kv.put(container=self._container,
                                      table_path=self._path,
                                      key=item_key,
                                      attributes=item_attributes)

        await self._verify_items(self._path, items)

        await self._client.kv.update(container=self._container,
                                     table_path=self._path,
                                     key='louise',
                                     attributes={
                                         'height': 130,
                                         'quip': 'i can smell fear on you'
                                     })

        response = await self._client.kv.get(container=self._container,
                                             table_path=self._path,
                                             key='louise',
                                             attribute_names=['__size', 'age', 'quip', 'height'])

        self.assertEqual(0, response.output.item['__size'])
        self.assertEqual(9, response.output.item['age'])
        self.assertEqual('i can smell fear on you', response.output.item['quip'])
        self.assertEqual(130, response.output.item['height'])

        # get items with filter expression
        response = await self._client.kv.scan(container=self._container,
                                              table_path=self._path,
                                              filter_expression="feature == 'singing'")
        self.assertEqual(1, len(response.output.items))

        # get items with segment / total_segments
        total_segments = 4
        total_items = []

        for segment in range(total_segments):
            received_items = await self._client.kv.new_cursor(container=self._container,
                                                              table_path=self._path,
                                                              segment=segment,
                                                              total_segments=total_segments).all()
            total_items.append(received_items)

        self.assertEqual(4, len(total_items))

        received_items = await self._client.kv.new_cursor(container=self._container,
                                                          table_path=self._path,
                                                          attribute_names=['age', 'feature'],
                                                          filter_expression='age > 15').all()

        self.assertEqual(2, len(received_items))
        for item in received_items:
            self.assertLess(15, item['age'])

        #
        # Increment age
        #

        await self._client.kv.update(container=self._container,
                                     table_path=self._path,
                                     key='louise',
                                     expression='age = age + 1')

        response = await self._client.kv.get(container=self._container,
                                             table_path=self._path,
                                             key='louise',
                                             attribute_names=['age'])

        self.assertEqual(10, response.output.item['age'])

    async def test_limit(self):

        for idx in range(100):
            await self._client.kv.put(container=self._container,
                                      table_path=self._path,
                                      key=f'key-{idx}',
                                      attributes={
                                          'attr': idx,
                                      })

        # limit using all()
        received_items = await self._client.kv.new_cursor(container=self._container,
                                                          table_path=self._path,
                                                          limit=30).all()

        self.assertEqual(len(received_items), 30)

    async def _delete_items(self, path, items):

        # delete items
        for item_key, _ in future.utils.viewitems(items):
            await self._client.kv.delete(container=self._container,
                                         table_path=path,
                                         key=item_key)

        # delete dir
        await self._client.object.delete(container=self._container,
                                         path=path)

    async def _verify_items(self, path, items):
        items_cursor = self._client.kv.new_cursor(container=self._container,
                                                  table_path=path,
                                                  attribute_names=['*'])

        received_items = await items_cursor.all()

        # TODO: verify contents
        self.assertEqual(len(items), len(received_items))

    def _compare_item_values(self, v1, v2):
        if isinstance(v1, array.array):
            # convert to list
            v1 = list(v1)

        if v1 != v2:
            self.fail('Values dont match')

    def _compare_item_types(self, v1, v2):
        if isinstance(v1, array.array):
            # convert to list
            v1 = list(v1)

        # can't guarantee strings as they might be converted to unicode
        if type(v1) is not str:
            self.assertEqual(type(v1), type(v2))


class TestRaiseForStatus(Test):

    def setUp(self):
        super(TestRaiseForStatus, self).setUp()

    async def test_always_raise_no_error(self):
        # should raise - since the status code is 500
        await self._client.container.list(self._container,
                                          '/',
                                          raise_for_status=v3io.dataplane.transport.RaiseForStatus.always)

    async def test_specific_status_code_match(self):
        # should raise - since the status code is 500
        await self._client.container.list(self._container, '/', raise_for_status=[200])

    async def test_specific_status_code_no_match(self):
        # should raise - since the status code is 500
        try:
            await self._client.container.list(self._container, '/', raise_for_status=[500])
        except Exception:
            return

        self.fail('Expected an exception')

    async def test_never_raise(self):
        await self._client.object.get(container=self._container,
                                      path='/non-existing',
                                      raise_for_status=v3io.aio.dataplane.RaiseForStatus.never)

    async def test_default_raise(self):
        try:
            await self._client.object.get(container=self._container, path='/non-existing')
        except Exception:
            return

        self.fail('Expected an exception')
