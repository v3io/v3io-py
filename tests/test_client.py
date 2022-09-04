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
import os.path
import unittest
import unittest.mock
import time
import array
import datetime

import future.utils

import v3io.common.helpers
import v3io.dataplane
import v3io.logger
import v3io.dataplane.response
import v3io.dataplane.output


class Test(unittest.TestCase):

    def setUp(self):
        self._client = v3io.dataplane.Client(transport_kind='httpclient',
                                             logger_verbosity='DEBUG',
                                             transport_verbosity='DEBUG')

        self._container = 'bigdata'
        self._test_parent_dir = os.environ.get("MATRIX_PYTHON_VERSION")
        if self._test_parent_dir is None:
            self._test_parent_dir = ''

    def tearDown(self):
        self._client.close()

    def _delete_dir(self, path):
        response = self._client.container.list(container=self._container,
                                               path=path,
                                               raise_for_status=v3io.dataplane.RaiseForStatus.never)

        if response.status_code == 404:
            return

        if response.status_code != 200:
            raise RuntimeError(response.body)

        for content in response.output.contents:
            self._client.object.delete(container=self._container, path=content.key)

        for common_prefixes in response.output.common_prefixes:
            self._client.object.delete(container=self._container,
                                       path=common_prefixes.prefix)


class TestContainer(Test):

    def setUp(self):
        super(TestContainer, self).setUp()
        self._path = os.path.join(self._test_parent_dir, 'v3io-py-test-container')

        # clean up
        self._delete_dir(self._path)

    def test_get_container_contents_invalid_path(self):
        response = self._client.container.list(container=self._container,
                                               path='/no-such-path',
                                               raise_for_status=v3io.dataplane.RaiseForStatus.never)
        self.assertEqual(404, response.status_code)
        self.assertIn('No such file', str(response.body))

    def test_get_container_contents(self):
        body = 'If you cannot do great things, do small things in a great way.'

        for object_index in range(5):
            self._client.object.put(container=self._container,
                                    path=os.path.join(self._path, 'object-{0}.txt'.format(object_index)),
                                    body=body)

        for object_index in range(5):
            self._client.object.put(container=self._container,
                                    path=os.path.join(self._path, 'dir-{0}/'.format(object_index)))

        response = self._client.container.list(container=self._container,
                                               path=self._path,
                                               get_all_attributes=True,
                                               directories_only=True)
        self.assertEqual(0, len(response.output.contents))
        self.assertNotEqual(0, len(response.output.common_prefixes))

        response = self._client.container.list(container=self._container,
                                               path=self._path,
                                               get_all_attributes=True)
        self.assertNotEqual(0, len(response.output.contents))
        self.assertNotEqual(0, len(response.output.common_prefixes))

        # clean up
        self._delete_dir(self._path)


class TestStream(Test):

    def setUp(self):
        super(TestStream, self).setUp()

        self._path = os.path.join(self._test_parent_dir, 'v3io-py-test-stream')

        # clean up
        self._client.stream.delete(container=self._container,
                                   stream_path=self._path,
                                   raise_for_status=[200, 204, 404])

    def test_delete_stream_with_cg(self):
        num_shards = 8

        # check that the stream doesn't exist
        self.assertFalse(self._stream_exists())

        # create a stream
        self._client.stream.create(container=self._container,
                                   stream_path=self._path,
                                   shard_count=num_shards)

        # write data to all shards so there are files
        for shard_id in range(num_shards):
            self._client.stream.put_records(container=self._container,
                                            stream_path=self._path,
                                            records=[
                                                {'shard_id': shard_id, 'data': 'data for shard {}'.format(shard_id)}
                                            ])

        # write several "consumer group state" files
        for cg_id in range(3):
            self._client.object.put(container=self._container,
                                    path=os.path.join(self._path, 'cg{}-state.json'.format(cg_id)))

        # check that the stream doesn't exist
        self.assertTrue(self._stream_exists())

        # delete the stream
        self._client.stream.delete(container=self._container, stream_path=self._path)

        # check that the stream doesn't exist
        self.assertFalse(self._stream_exists())

    def test_stream(self):

        # create a stream w/8 shards
        self._client.stream.create(container=self._container,
                                   stream_path=self._path,
                                   shard_count=8)

        records = [
            {'shard_id': 1, 'data': 'first shard record #1'},
            {'shard_id': 1, 'data': 'first shard record #2', 'client_info': bytearray(b'some info')},
            {'shard_id': 10, 'data': 'invalid shard record #1'},
            {'shard_id': 2, 'data': 'second shard record #1'},
            {'data': 'some shard record #1'},
        ]

        response = self._client.stream.put_records(container=self._container,
                                                   stream_path=self._path,
                                                   records=records)
        self.assertEqual(1, response.output.failed_record_count)

        for response_record_index, response_record in enumerate(response.output.records):
            if response_record_index == 2:
                self.assertIsNotNone(response_record.error_code)
            else:
                self.assertIsNone(response_record.error_code)

        response = self._client.stream.seek(container=self._container,
                                            stream_path=self._path,
                                            shard_id=1,
                                            seek_type='EARLIEST')

        self.assertNotEqual('', response.output.location)

        response = self._client.stream.get_records(container=self._container,
                                                   stream_path=self._path,
                                                   shard_id=1,
                                                   location=response.output.location)

        self.assertEqual(2, len(response.output.records))
        self.assertEqual(records[0]['data'], response.output.records[0].data.decode('utf-8'))
        self.assertEqual(records[1]['data'], response.output.records[1].data.decode('utf-8'))
        self.assertEqual(records[1]['client_info'], response.output.records[1].client_info)

        # update the stream by adding 8 shards to it
        self._client.stream.update(container=self._container,
                                   stream_path=self._path,
                                   shard_count=16)

        records = [
            {'shard_id': 10, 'data': 'Now valid shard record #1'},
        ]

        response = self._client.stream.put_records(container=self._container,
                                                   stream_path=self._path,
                                                   records=records)

        self.assertEqual(0, response.output.failed_record_count)

        self._client.stream.delete(container=self._container,
                                   stream_path=self._path)

    def _stream_exists(self):
        response = self._client.stream.describe(container=self._container,
                                                stream_path=self._path,
                                                raise_for_status=v3io.dataplane.RaiseForStatus.never)
        return response.status_code == 200


class TestObject(Test):

    def setUp(self):
        super(TestObject, self).setUp()

        self._object_dir = os.path.join(self._test_parent_dir, 'v3io-py-test-object')
        self._object_path = self._object_dir + '/obj ect.txt'

        # clean up
        self._delete_dir(self._object_dir)

    def test_object(self):
        contents = 'vegans are better than everyone'

        response = self._client.object.get(container=self._container,
                                           path=self._object_path,
                                           raise_for_status=v3io.dataplane.RaiseForStatus.never)

        self.assertEqual(404, response.status_code)

        # put contents to some object
        self._client.object.put(container=self._container,
                                path=self._object_path,
                                body=contents)

        # get the object contents
        response = self._client.object.get(container=self._container,
                                           path=self._object_path)

        if not isinstance(response.body, str):
            response.body = response.body.decode('utf-8')

        self.assertEqual(response.body, contents)

        # get the head of the object
        response = self._client.object.head(container=self._container,
                                           path=self._object_path)

        self.assertIn(('Content-Length', str(len(contents))), response.headers.items())

        # get the head of the dir-object
        response = self._client.object.head(container=self._container,
                                           path=self._object_dir)

        self.assertIn(('Content-Length', str(0)), response.headers.items())

        # delete the object
        self._client.object.delete(container=self._container,
                                   path=self._object_path)

        # get again
        response = self._client.object.get(container=self._container,
                                           path=self._object_path,
                                           raise_for_status=v3io.dataplane.RaiseForStatus.never)

        self.assertEqual(404, response.status_code)

    def test_append(self):
        contents = [
            'First part',
            'Second part',
            'Third part',
        ]

        # put the contents into the object
        for content in contents:
            self._client.object.put(container=self._container,
                                    path=self._object_path,
                                    body=content,
                                    append=True)

        # get the contents
        response = self._client.object.get(container=self._container,
                                           path=self._object_path)

        self.assertEqual(response.body.decode('utf-8'), ''.join(contents))

    def test_get_offset(self):
        self._client.object.put(container=self._container,
                                path=self._object_path,
                                body='1234567890')

        # get the contents without limit
        response = self._client.object.get(container=self._container,
                                           path=self._object_path,
                                           offset=4)

        self.assertEqual(response.body.decode('utf-8'), '567890')

        # get the contents with limit
        response = self._client.object.get(container=self._container,
                                           path=self._object_path,
                                           offset=4,
                                           num_bytes=3)

        self.assertEqual(response.body.decode('utf-8'), '567')

    def test_batch(self):

        def _object_path(idx):
            return self._object_dir + '/object' + str(idx)

        def _object_contents(idx):
            return 'object-' + str(idx)

        num_objects = 16

        for object_idx in range(num_objects):
            self._client.batch.object.put(self._container,
                                          _object_path(object_idx),
                                          body=_object_contents(object_idx))

        responses = self._client.batch.wait()

        for response in responses:
            self.assertEqual(200, response.status_code)

        for object_idx in range(num_objects):
            self._client.batch.object.get(self._container, _object_path(object_idx))

        responses = self._client.batch.wait()

        for response_idx, response in enumerate(responses):
            self.assertEqual(200, response.status_code)
            self.assertEqual(_object_contents(response_idx), response.body.decode('utf-8'))


class TestSchema(Test):

    def setUp(self):
        super(TestSchema, self).setUp()

        self._schema_dir = os.path.join(self._test_parent_dir, 'v3io-py-test-schema')
        self._schema_path = os.path.join(self._schema_dir, '.#schema')

        # clean up
        self._delete_dir(self._schema_dir)

    def test_create_schema(self):
        # write schema
        self._client.kv.create_schema(container=self._container,
                                      table_path=self._schema_dir,
                                      key='key_field',
                                      fields=[
                                          {
                                              'name': 'key_field',
                                              'type': 'string',
                                              'nullable': False
                                          },
                                          {
                                              'name': 'data_field_0',
                                              'type': 'long',
                                              'nullable': True
                                          },
                                          {
                                              'name': 'data_field_1',
                                              'type': 'double',
                                              'nullable': True
                                          },
                                      ])

        # write to test the values in the UI (requires breaking afterwards)
        items = {
            'a': {'data_field_0': 30, 'data_field_1': 100},
            'b': {'data_field_0': 300, 'data_field_1': 1000},
            'c': {'data_field_0': 3000, 'data_field_1': 10000},
        }

        for item_key, item_attributes in future.utils.viewitems(items):
            self._client.kv.put(container=self._container,
                                table_path=self._schema_dir,
                                key=item_key,
                                attributes=item_attributes)

        # verify the scehma
        response = self._client.object.get(container=self._container,
                                           path=self._schema_path,
                                           raise_for_status=v3io.dataplane.RaiseForStatus.never)

        # find a way to assert this without assuming serialization order
        # self.assertEqual(
        #     '{"hashingBucketNum":0,"key":"key_field","fields":[{"name":"key_field","type":"string","nullable":false},'
        #     '{"name":"data_field_0","type":"long","nullable":true},{"name":"data_field_1","type":"double"'
        #     ',"nullable":true}]}',
        #     response.body.decode('utf-8'))


class TestKv(Test):

    def setUp(self):
        super(TestKv, self).setUp()

        self._path = os.path.join(self._test_parent_dir, 'some_dir/v3io-py-test-emd')
        self._delete_dir(self._path)

    def test_kv_array(self):
        item_key = 'item_with_arrays'
        item = {
            'list_with_ints': [1, 2, 3],
            'list_with_floats': [10.25, 20.25, 30.25],
        }

        self._client.kv.put(container=self._container,
                            table_path=self._path,
                            key=item_key,
                            attributes=item)

        for attribute_name in item.keys():
            self._client.kv.update(container=self._container,
                                   table_path=self._path,
                                   key=item_key,
                                   expression=f'{attribute_name}[1]={attribute_name}[1]*2')

        # get the item
        response = self._client.kv.get(container=self._container,
                                       table_path=self._path,
                                       key=item_key)

        for attribute_name in item.keys():
            self.assertEqual(response.output.item[attribute_name][1], item[attribute_name][1] * 2)

    def test_kv_values(self):

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

        self._client.kv.put(container=self._container,
                            table_path=self._path,
                            key=item_key,
                            attributes=item[item_key])

        response = self._client.kv.get(container=self._container,
                                       table_path=self._path,
                                       key=item_key)

        self.assertEqual(len(item[item_key].keys()), len(response.output.item.keys()))

        for key, value in response.output.item.items():
            self._compare_item_values(item[item_key][key], value)

        for key, value in item[item_key].items():
            self._compare_item_types(item[item_key][key], response.output.item[key])

    def test_kv(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        for item_key, item_attributes in future.utils.viewitems(items):
            self._client.kv.put(container=self._container,
                                table_path=self._path,
                                key=item_key,
                                attributes=item_attributes)

        self._verify_items(self._path, items)

        self._client.kv.update(container=self._container,
                               table_path=self._path,
                               key='louise',
                               attributes={
                                   'height': 130,
                                   'quip': 'i can smell fear on you'
                               })

        response = self._client.kv.get(container=self._container,
                                       table_path=self._path,
                                       key='louise',
                                       attribute_names=['__size', 'age', 'quip', 'height'])

        self.assertEqual(0, response.output.item['__size'])
        self.assertEqual(9, response.output.item['age'])
        self.assertEqual('i can smell fear on you', response.output.item['quip'])
        self.assertEqual(130, response.output.item['height'])

        # get items with filter expression
        response = self._client.kv.scan(container=self._container,
                                        table_path=self._path,
                                        filter_expression="feature == 'singing'")
        self.assertEqual(1, len(response.output.items))

        # get items with segment / total_segments
        total_segments = 4
        total_items = []

        for segment in range(total_segments):
            received_items = self._client.kv.new_cursor(container=self._container,
                                                        table_path=self._path,
                                                        segment=segment,
                                                        total_segments=total_segments).all()
            total_items.append(received_items)

        self.assertEqual(4, len(total_items))

        received_items = self._client.kv.new_cursor(container=self._container,
                                                    table_path=self._path,
                                                    attribute_names=['age', 'feature'],
                                                    filter_expression='age > 15').all()

        self.assertEqual(2, len(received_items))
        for item in received_items:
            self.assertLess(15, item['age'])

        #
        # Increment age
        #

        self._client.kv.update(container=self._container,
                               table_path=self._path,
                               key='louise',
                               expression='age = age + 1')

        response = self._client.kv.get(container=self._container,
                                       table_path=self._path,
                                       key='louise',
                                       attribute_names=['age'])

        self.assertEqual(10, response.output.item['age'])

    def test_limit(self):
        for idx in range(100):
            self._client.kv.put(container=self._container,
                                table_path=self._path,
                                key=f'key-{idx}',
                                attributes={
                                    'attr': idx,
                                })

        # limit using all()
        received_items = self._client.kv.new_cursor(container=self._container,
                                                    table_path=self._path,
                                                    limit=30).all()

        self.assertEqual(len(received_items), 30)

    def test_batch(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        # put the item in a batch
        for item_key, item_attributes in future.utils.viewitems(items):
            self._client.batch.kv.put(container=self._container,
                                      table_path=self._path,
                                      key=item_key,
                                      attributes=item_attributes)

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

        for item_key in items.keys():
            self._client.batch.kv.get(container=self._container,
                                      table_path=self._path,
                                      key=item_key,
                                      attribute_names=['__size', 'age'])

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

    def _delete_items(self, path, items):

        # delete items
        for item_key, _ in future.utils.viewitems(items):
            self._client.kv.delete(container=self._container,
                                   table_path=path,
                                   key=item_key)

        # delete dir
        self._client.object.delete(container=self._container,
                                   path=path)

    def _verify_items(self, path, items):
        items_cursor = self._client.kv.new_cursor(container=self._container,
                                                  table_path=path,
                                                  attribute_names=['*'])

        received_items = items_cursor.all()

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

    def test_always_raise_no_error(self):
        # should raise - since the status code is 500
        self._client.container.list(self._container,
                                    '/',
                                    raise_for_status=v3io.dataplane.transport.RaiseForStatus.always)

    def test_specific_status_code_match(self):
        # should raise - since the status code is 500
        self._client.container.list(self._container, '/', raise_for_status=[200])

    def test_specific_status_code_no_match(self):
        # should raise - since the status code is 500
        self.assertRaises(Exception, self._client.container.list, self._container, '/', raise_for_status=[500])

    def test_never_raise(self):
        self._client.object.get(container=self._container,
                                path='/non-existing',
                                raise_for_status=v3io.dataplane.RaiseForStatus.never)

    def test_default_raise(self):
        self.assertRaises(Exception, self._client.object.get, container=self._container, path='/non-existing')


class TestBatchRaiseForStatus(Test):

    def setUp(self):
        super(TestBatchRaiseForStatus, self).setUp()
        self._object_dir = '/v3io-py-test-batch-raise'

        # clean up
        self._delete_dir(self._object_dir)

    def test_raise(self):

        def _object_path(idx):
            return self._object_dir + '/object' + str(idx)

        def _object_contents(idx):
            return 'object-' + str(idx)

        num_objects = 4
        err_idx = 1

        for object_idx in range(num_objects):
            self._client.batch.object.put(self._container,
                                          _object_path(object_idx),
                                          body=_object_contents(object_idx))

        responses = self._client.batch.wait()

        for object_idx in range(num_objects):

            # inject an error
            if object_idx == err_idx:
                object_idx = 10

            self._client.batch.object.get(self._container, _object_path(object_idx))

        self.assertRaises(Exception, self._client.batch.wait)

        # do it again, only this time don't raise
        for object_idx in range(num_objects):

            # inject an error
            if object_idx == 1:
                object_idx = 10

            self._client.batch.object.get(self._container, _object_path(object_idx))

        responses = self._client.batch.wait(raise_for_status=v3io.dataplane.RaiseForStatus.never)

        for response_idx, response in enumerate(responses):
            if response_idx == err_idx:
                self.assertEqual(404, response.status_code)
            else:
                self.assertEqual(200, response.status_code)


class TestConnectonErrorRecovery(Test):

    def setUp(self):
        super(TestConnectonErrorRecovery, self).setUp()

        self._object_dir = os.path.join(self._test_parent_dir, 'v3io-py-test-connection-error')
        self._object_path = self._object_dir + '/object.txt'

        self._kv_path = 'some_dir/v3io-py-test-emd'
        self._delete_dir(self._kv_path)

        # clean up
        self._delete_dir(self._object_dir)

    @unittest.skip("Manually executed")
    def test_object(self):

        for i in range(100):
            body = 'iteration {}'.format(i)

            if i == 10:
                self._restart_webapi()

            # put contents to some object
            self._client.object.put(container=self._container,
                                    path=self._object_path,
                                    body=body)

            response = self._client.object.get(container=self._container,
                                               path=self._object_path)

            if not isinstance(response.body, str):
                response.body = response.body.decode('utf-8')

            self.assertEqual(response.body, body)

            time.sleep(0.1)

    @unittest.skip("Manually executed")
    def test_kv_batch(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        # put the item in a batch
        for item_key, item_attributes in future.utils.viewitems(items):
            self._client.batch.put_item(container=self._container,
                                        path=v3io.common.helpers.url_join(self._kv_path, item_key),
                                        attributes=item_attributes)

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

        self._restart_webapi()

        for item_key in items.keys():
            self._client.batch.get_item(container=self._container,
                                        path=v3io.common.helpers.url_join(self._kv_path, item_key),
                                        attribute_names=['__size', 'age'])

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

    def _restart_webapi(self):
        print('Restart webapi now')
        time.sleep(15)


class TestCustomTransport(unittest.TestCase):

    def test_verifier_transport(self):
        container_name = 'some_container'

        #
        # Register a set of verifiers
        #

        def _verify_object_get(request):
            # verify some stuff from the request
            self.assertEqual(request.container, container_name)
            self.assertEqual(request.path, os.path.join(os.sep, container_name, 'some/path'))

            # return a mocked response
            return unittest.mock.MagicMock(status_code=200,
                                           body='some body')

        def _verify_kv_get(request):
            # verify some stuff from the request
            self.assertEqual(request.container, container_name)
            self.assertEqual(request.path, os.path.join(os.sep, container_name, 'some/table/path/some_item_key'))

            # prepare and output mock
            output = unittest.mock.MagicMock(item={
                'some_key': 'some_value'
            })

            # prepare a response mock
            return unittest.mock.MagicMock(output=output)

        # create a verifier transport. pass it a set of request verifiers
        verifier_transport = v3io.dataplane.transport.verifier.Transport(request_verifiers=[
            _verify_object_get,
            _verify_kv_get,
        ])

        #
        # Run some flow
        #

        # create a client with a verifier transport
        client = v3io.dataplane.Client(transport_kind=verifier_transport)

        # do an object get
        response = client.object.get(container='some_container', path='some/path')

        # verify that we got some body from the verifier
        self.assertEqual(response.body, 'some body')
        self.assertEqual(response.status_code, 200)

        # do an item get
        response = client.kv.get(container=container_name,
                                 table_path='some/table/path',
                                 key='some_item_key')

        # verify that we got a proper
        self.assertEqual(response.output.item['some_key'], 'some_value')
