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
import time
import array
import datetime

import future.utils

import v3io.common.helpers
import v3io.dataplane
import v3io.logger


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
        response = self._client.get_container_contents(container=self._container,
                                                       path=path,
                                                       raise_for_status=v3io.dataplane.RaiseForStatus.never)

        if response.status_code == 404:
            return

        if response.status_code != 200:
            raise RuntimeError(response.body)

        for content in response.output.contents:
            self._client.delete_object(container=self._container, path=content.key)

        for common_prefixes in response.output.common_prefixes:
            self._client.delete_object(container=self._container,
                                       path=common_prefixes.prefix)


class TestContainer(Test):

    def setUp(self):
        super(TestContainer, self).setUp()
        self._path = os.path.join(self._test_parent_dir, 'v3io-py-test-container')

        # clean up
        self._delete_dir(self._path)

    def test_get_containers(self):
        response = self._client.get_containers()
        self.assertGreater(len(response.output.containers), 0)

    def test_get_container_contents_invalid_path(self):
        response = self._client.get_container_contents(container=self._container,
                                                       path='/no-such-path',
                                                       raise_for_status=v3io.dataplane.RaiseForStatus.never)
        self.assertEqual(404, response.status_code)
        self.assertIn('No such file', str(response.body))

    def test_get_container_contents(self):
        body = 'If you cannot do great things, do small things in a great way.'

        for object_index in range(5):
            self._client.put_object(container=self._container,
                                    path=os.path.join(self._path, 'object-{0}.txt'.format(object_index)),
                                    body=body)

        for object_index in range(5):
            self._client.put_object(container=self._container,
                                    path=os.path.join(self._path, 'dir-{0}/'.format(object_index)))

        response = self._client.get_container_contents(container=self._container,
                                                       path=self._path,
                                                       get_all_attributes=True,
                                                       directories_only=True)
        self.assertEqual(0, len(response.output.contents))
        self.assertNotEqual(0, len(response.output.common_prefixes))

        response = self._client.get_container_contents(container=self._container,
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
        self._client.delete_stream(container=self._container,
                                   path=self._path,
                                   raise_for_status=[200, 204, 404])

    def test_delete_stream_with_cg(self):
        num_shards = 8

        # check that the stream doesn't exist
        self.assertFalse(self._stream_exists())

        # create a stream
        self._client.create_stream(container=self._container,
                                   path=self._path,
                                   shard_count=num_shards)

        # write data to all shards so there are files
        for shard_id in range(num_shards):
            self._client.put_records(container=self._container,
                                     path=self._path,
                                     records=[
                                         {'shard_id': shard_id, 'data': 'data for shard {}'.format(shard_id)}
                                     ])

        # write several "consumer group state" files
        for cg_id in range(3):
            self._client.put_object(container=self._container,
                                    path=os.path.join(self._path, 'cg{}-state.json'.format(cg_id)))

        # check that the stream doesn't exist
        self.assertTrue(self._stream_exists())

        # delete the stream
        self._client.delete_stream(container=self._container, path=self._path)

        # check that the stream doesn't exist
        self.assertFalse(self._stream_exists())

    def test_stream(self):

        # create a stream w/8 shards
        self._client.create_stream(container=self._container,
                                   path=self._path,
                                   shard_count=8)

        records = [
            {'shard_id': 1, 'data': 'first shard record #1'},
            {'shard_id': 1, 'data': 'first shard record #2', 'client_info': bytearray(b'some info')},
            {'shard_id': 10, 'data': 'invalid shard record #1'},
            {'shard_id': 2, 'data': 'second shard record #1'},
            {'data': 'some shard record #1'},
        ]

        response = self._client.put_records(container=self._container,
                                            path=self._path,
                                            records=records)
        self.assertEqual(1, response.output.failed_record_count)

        for response_record_index, response_record in enumerate(response.output.records):
            if response_record_index == 2:
                self.assertIsNotNone(response_record.error_code)
            else:
                self.assertIsNone(response_record.error_code)

        shard_path = self._path + '/1'

        response = self._client.seek_shard(container=self._container,
                                           path=shard_path,
                                           seek_type='EARLIEST')

        self.assertNotEqual('', response.output.location)

        response = self._client.get_records(container=self._container,
                                            path=shard_path,
                                            location=response.output.location)

        self.assertEqual(2, len(response.output.records))
        self.assertEqual(records[0]['data'], response.output.records[0].data.decode('utf-8'))
        self.assertEqual(records[1]['data'], response.output.records[1].data.decode('utf-8'))
        self.assertEqual(records[1]['client_info'], response.output.records[1].client_info)

        # update the stream by adding 8 shards to it
        self._client.update_stream(container=self._container,
                                   path=self._path,
                                   shard_count=16)

        records = [
            {'shard_id': 10, 'data': 'Now valid shard record #1'},
        ]

        response = self._client.put_records(container=self._container,
                                            path=self._path,
                                            records=records)

        self.assertEqual(0, response.output.failed_record_count)

        self._client.delete_stream(container=self._container,
                                   path=self._path)

    def _stream_exists(self):
        response = self._client.describe_stream(container=self._container,
                                                path=self._path,
                                                raise_for_status=v3io.dataplane.RaiseForStatus.never)
        return response.status_code == 200


class TestObject(Test):

    def setUp(self):
        super(TestObject, self).setUp()

        self._object_dir = os.path.join(self._test_parent_dir, '/v3io-py-test-object')
        self._object_path = self._object_dir + '/object.txt'

        # clean up
        self._delete_dir(self._object_dir)

    def test_object(self):
        contents = 'vegans are better than everyone'

        response = self._client.get_object(container=self._container,
                                           path=self._object_path,
                                           raise_for_status=v3io.dataplane.RaiseForStatus.never)

        self.assertEqual(404, response.status_code)

        # put contents to some object
        self._client.put_object(container=self._container,
                                path=self._object_path,
                                body=contents)

        # get the contents
        response = self._client.get_object(container=self._container,
                                           path=self._object_path)

        if not isinstance(response.body, str):
            response.body = response.body.decode('utf-8')

        self.assertEqual(response.body, contents)

        # delete the object
        self._client.delete_object(container=self._container,
                                   path=self._object_path)

        # get again
        response = self._client.get_object(container=self._container,
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
            self._client.put_object(container=self._container,
                                    path=self._object_path,
                                    body=content,
                                    append=True)

        # get the contents
        response = self._client.get_object(container=self._container,
                                           path=self._object_path)

        self.assertEqual(response.body.decode('utf-8'), ''.join(contents))

    def test_get_offset(self):
        self._client.put_object(container=self._container,
                                path=self._object_path,
                                body='1234567890')

        # get the contents without limit
        response = self._client.get_object(container=self._container,
                                           path=self._object_path,
                                           offset=4)

        self.assertEqual(response.body.decode('utf-8'), '567890')

        # get the contents with limit
        response = self._client.get_object(container=self._container,
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
            self._client.batch.put_object(self._container,
                                          _object_path(object_idx),
                                          body=_object_contents(object_idx))

        responses = self._client.batch.wait()

        for response in responses:
            self.assertEqual(200, response.status_code)

        for object_idx in range(num_objects):
            self._client.batch.get_object(self._container, _object_path(object_idx))

        responses = self._client.batch.wait()

        for response_idx, response in enumerate(responses):
            self.assertEqual(200, response.status_code)
            self.assertEqual(_object_contents(response_idx), response.body.decode('utf-8'))


class TestSchema(Test):

    def setUp(self):
        super(TestSchema, self).setUp()

        self._schema_dir = os.path.join(self._test_parent_dir, '/v3io-py-test-schema')
        self._schema_path = os.path.join(self._schema_dir, '.#schema')

        # clean up
        self._delete_dir(self._schema_dir)

    def test_create_schema(self):
        # write schema
        self._client.create_schema(container=self._container,
                                   path=self._schema_dir,
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
            self._client.put_item(container=self._container,
                                  path=v3io.common.helpers.url_join(self._schema_dir, item_key),
                                  attributes=item_attributes)

        # verify the scehma
        response = self._client.get_object(container=self._container,
                                           path=self._schema_path,
                                           raise_for_status=v3io.dataplane.RaiseForStatus.never)

        # find a way to assert this without assuming serialization order
        # self.assertEqual(
        #     '{"hashingBucketNum":0,"key":"key_field","fields":[{"name":"key_field","type":"string","nullable":false},'
        #     '{"name":"data_field_0","type":"long","nullable":true},{"name":"data_field_1","type":"double"'
        #     ',"nullable":true}]}',
        #     response.body.decode('utf-8'))


class TestEmd(Test):

    def setUp(self):
        super(TestEmd, self).setUp()

        self._path = os.path.join(self._test_parent_dir, 'some_dir/v3io-py-test-emd')
        self._delete_dir(v3io.common.helpers.url_join(self._path, 'my_table'))
        self._delete_dir(self._path)

    def test_emd_array(self):
        item_key = 'item_with_arrays'
        item = {
            'list_with_ints': [1, 2, 3],
            'list_with_floats': [10.25, 20.25, 30.25],
        }

        item_path = v3io.common.helpers.url_join(self._path, item_key)

        self._client.put_item(container=self._container,
                              path=item_path,
                              attributes=item)

        for attribute_name in item.keys():
            self._client.update_item(container=self._container,
                                     path=item_path,
                                     expression=f'{attribute_name}[1]={attribute_name}[1]*2')

        # get the item
        response = self._client.get_item(container=self._container, path=item_path)

        for attribute_name in item.keys():
            self.assertEqual(response.output.item[attribute_name][1], item[attribute_name][1] * 2)

    def test_emd_values(self):

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

        self._client.put_item(container=self._container,
                              path=v3io.common.helpers.url_join(self._path, item_key),
                              attributes=item[item_key])

        response = self._client.get_item(container=self._container,
                                         path=v3io.common.helpers.url_join(self._path, item_key))

        self.assertEqual(len(item[item_key].keys()), len(response.output.item.keys()))

        for key, value in response.output.item.items():
            self._compare_item_values(item[item_key][key], value)

        for key, value in item[item_key].items():
            self._compare_item_types(item[item_key][key], response.output.item[key])

    def test_emd_with_table_name(self):
        self._client.put_item(container=self._container,
                              path=v3io.common.helpers.url_join(self._path, 'my_table', 'my_key'),
                              attributes={
                                  'a': 's',
                                  'i': 30
                              })

        items = []

        response = self._client.get_items(container=self._container,
                                          path=self._path,
                                          table_name='my_table')

        items.extend(response.output.items)

        while response.output.next_marker and not response.output.last:
            response = self._client.get_items(container=self._container,
                                              path=self._path,
                                              table_name='my_table',
                                              marker=response.output.next_marker)
            items.extend(response.output.items)

        self.assertEqual(len(items), 1)

    def test_emd(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        for item_key, item_attributes in future.utils.viewitems(items):
            self._client.put_item(container=self._container,
                                  path=v3io.common.helpers.url_join(self._path, item_key),
                                  attributes=item_attributes)

        self._verify_items(self._path, items)

        self._client.update_item(container=self._container,
                                 path=v3io.common.helpers.url_join(self._path, 'louise'),
                                 attributes={
                                     'height': 130,
                                     'quip': 'i can smell fear on you'
                                 })

        response = self._client.get_item(container=self._container,
                                         path=v3io.common.helpers.url_join(self._path, 'louise'),
                                         attribute_names=['__size', 'age', 'quip', 'height'])

        self.assertEqual(0, response.output.item['__size'])
        self.assertEqual(9, response.output.item['age'])
        self.assertEqual('i can smell fear on you', response.output.item['quip'])
        self.assertEqual(130, response.output.item['height'])

        # get items with filter expression
        response = self._client.get_items(container=self._container,
                                          path=self._path,
                                          filter_expression="feature == 'singing'")
        self.assertEqual(1, len(response.output.items))

        # get items with segment / total_segments
        total_segments = 4
        total_items = []

        for segment in range(total_segments):
            received_items = self._client.new_items_cursor(container=self._container,
                                                           path=self._path,
                                                           segment=segment,
                                                           total_segments=total_segments).all()
            total_items.append(received_items)

        self.assertEqual(4, len(total_items))

        # with limit = 0
        received_items = self._client.new_items_cursor(container=self._container,
                                                       path=self._path,
                                                       attribute_names=['age', 'feature'],
                                                       filter_expression='age > 15',
                                                       limit=0).all()

        self.assertEqual(0, len(received_items))

        received_items = self._client.new_items_cursor(container=self._container,
                                                       path=self._path,
                                                       attribute_names=['age', 'feature'],
                                                       filter_expression='age > 15').all()

        self.assertEqual(2, len(received_items))
        for item in received_items:
            self.assertLess(15, item['age'])

        #
        # Increment age
        #

        self._client.update_item(container=self._container,
                                 path=v3io.common.helpers.url_join(self._path, 'louise'),
                                 expression='age = age + 1')

        response = self._client.get_item(container=self._container,
                                         path=v3io.common.helpers.url_join(self._path, 'louise'),
                                         attribute_names=['age'])

        self.assertEqual(10, response.output.item['age'])

    def test_put_items(self):
        items = {
            'bob': {
                'age': 42,
                'feature': 'mustache',
                'female': False
            },
            'linda': {
                'age': 40,
                'feature': 'singing',
                'female': True,
                'some_blob': bytearray('\x00\x11\x00\x11', encoding='utf-8')
            },
        }

        response = self._client.put_items(container=self._container,
                                          path=self._path,
                                          items=items)

        self.assertTrue(response.success)

        self._verify_items(self._path, items)

        # delete an item
        self._client.delete_item(container=self._container, path=self._path + '/linda')
        del(items['linda'])

        self._verify_items(self._path, items)

    def test_put_items_with_error(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 40, 'feature': 'singing'},
            'invalid': {'__name': 'foo', 'feature': 'singing'}
        }

        response = self._client.put_items(container=self._container,
                                          path=self._path,
                                          items=items,
                                          raise_for_status=v3io.dataplane.RaiseForStatus.never)

        self.assertFalse(response.success)

        # first two should've passed
        response.responses[0].raise_for_status()
        response.responses[1].raise_for_status()
        self.assertEqual(403, response.responses[2].status_code)

        # remove invalid so we can verify
        del items['invalid']

        self._verify_items(self._path, items)

    def test_batch(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        # put the item in a batch
        for item_key, item_attributes in future.utils.viewitems(items):
            self._client.batch.put_item(container=self._container,
                                        path=v3io.common.helpers.url_join(self._path, item_key),
                                        attributes=item_attributes)

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

        for item_key in items.keys():
            self._client.batch.get_item(container=self._container,
                                        path=v3io.common.helpers.url_join(self._path, item_key),
                                        attribute_names=['__size', 'age'])

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

    def _delete_items(self, path, items):

        # delete items
        for item_key, _ in future.utils.viewitems(items):
            self._client.delete_object(container=self._container,
                                       path=v3io.common.helpers.url_join(path, item_key))

        # delete dir
        self._client.delete_object(container=self._container,
                                   path=path)

    def _verify_items(self, path, items):
        items_cursor = self._client.new_items_cursor(container=self._container,
                                                     path=path,
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
        self._client.get_containers(raise_for_status=v3io.dataplane.transport.RaiseForStatus.always)

    def test_specific_status_code_match(self):
        # should raise - since the status code is 500
        self._client.get_containers(raise_for_status=[200])

    def test_specific_status_code_no_match(self):
        # should raise - since the status code is 500
        self.assertRaises(Exception, self._client.get_containers, raise_for_status=[500])

    def test_never_raise(self):
        self._client.get_object(container=self._container,
                                path='/non-existing',
                                raise_for_status=v3io.dataplane.RaiseForStatus.never)

    def test_default_raise(self):
        self.assertRaises(Exception, self._client.get_object, container=self._container, path='/non-existing')


class TestBatchRaiseForStatus(Test):

    def setUp(self):
        super(TestBatchRaiseForStatus, self).setUp()
        self._object_dir = os.path.join(self._test_parent_dir, 'some_dir/v3io-py-test-batch-raise')

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
            self._client.batch.put_object(self._container,
                                          _object_path(object_idx),
                                          body=_object_contents(object_idx))

        responses = self._client.batch.wait()

        for object_idx in range(num_objects):

            # inject an error
            if object_idx == err_idx:
                object_idx = 10

            self._client.batch.get_object(self._container, _object_path(object_idx))

        self.assertRaises(Exception, self._client.batch.wait)

        # do it again, only this time don't raise
        for object_idx in range(num_objects):

            # inject an error
            if object_idx == 1:
                object_idx = 10

            self._client.batch.get_object(self._container, _object_path(object_idx))

        responses = self._client.batch.wait(raise_for_status=v3io.dataplane.RaiseForStatus.never)

        for response_idx, response in enumerate(responses):
            if response_idx == err_idx:
                self.assertEqual(404, response.status_code)
            else:
                self.assertEqual(200, response.status_code)


class TestConnectonErrorRecovery(Test):

    def setUp(self):
        super(TestConnectonErrorRecovery, self).setUp()

        self._object_dir = os.path.join(self._test_parent_dir, '/v3io-py-test-connection-error')
        self._object_path = self._object_dir + '/object.txt'

        self._emd_path = 'some_dir/v3io-py-test-emd'
        self._delete_dir(self._emd_path)

        # clean up
        self._delete_dir(self._object_dir)

    @unittest.skip("Manually executed")
    def test_object(self):

        for i in range(100):
            body = 'iteration {}'.format(i)

            if i == 10:
                self._restart_webapi()

            # put contents to some object
            self._client.put_object(container=self._container,
                                    path=self._object_path,
                                    body=body)

            response = self._client.get_object(container=self._container,
                                               path=self._object_path)

            if not isinstance(response.body, str):
                response.body = response.body.decode('utf-8')

            self.assertEqual(response.body, body)

            time.sleep(0.1)

    @unittest.skip("Manually executed")
    def test_emd_batch(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        # put the item in a batch
        for item_key, item_attributes in future.utils.viewitems(items):
            self._client.batch.put_item(container=self._container,
                                        path=v3io.common.helpers.url_join(self._emd_path, item_key),
                                        attributes=item_attributes)

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

        self._restart_webapi()

        for item_key in items.keys():
            self._client.batch.get_item(container=self._container,
                                        path=v3io.common.helpers.url_join(self._emd_path, item_key),
                                        attribute_names=['__size', 'age'])

        responses = self._client.batch.wait()
        for response in responses:
            self.assertEqual(200, response.status_code)

    def _restart_webapi(self):
        print('Restart webapi now')
        time.sleep(15)
