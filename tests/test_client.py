import os.path
import unittest
import sys

import future.utils

import v3io.common.helpers
import v3io.dataplane
import v3io.logger


class Test(unittest.TestCase):

    def setUp(self):
        self._logger = v3io.logger.Logger()
        self._logger.set_handler('stdout', sys.stdout, v3io.logger.HumanReadableFormatter())
        self._client = v3io.dataplane.Client(self._logger, transport_kind='httpclient')

        self._container = 'bigdata'

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
        self._path = 'v3io-py-test-container'

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

        self._path = 'v3io-py-test-stream'

        # clean up
        self._client.delete_stream(container=self._container,
                                   path=self._path,
                                   raise_for_status=[200, 204, 404])

    def test_stream(self):

        # create a stream w/8 shards
        self._client.create_stream(container=self._container,
                                   path=self._path,
                                   shard_count=8)

        records = [
            {'shard_id': 1, 'data': 'first shard record #1'},
            {'shard_id': 1, 'data': 'first shard record #2'},
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
        self.assertEqual('first shard record #1', response.output.records[0].data.decode('utf-8'))
        self.assertEqual('first shard record #2', response.output.records[1].data.decode('utf-8'))

        self._client.delete_stream(container=self._container,
                                   path=self._path)


class TestObject(Test):

    def setUp(self):
        super(TestObject, self).setUp()

        self._object_dir = '/v3io-py-test-object'
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
            return self._object_dir + f'/object{idx}'

        def _object_contents(idx):
            return f'object-{idx}'

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

        self._schema_dir = '/v3io-py-test-schema'
        self._schema_path = os.path.join(self._schema_dir, '.%23schema')

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

        self.assertEqual(
            '{"hashingBucketNum":0,"key":"key_field","fields":[{"name":"key_field","type":"string","nullable":false},'
            '{"name":"data_field_0","type":"long","nullable":true},{"name":"data_field_1","type":"double"'
            ',"nullable":true}]}',
            response.body.decode('utf-8'))


class TestEmd(Test):

    def setUp(self):
        super(TestEmd, self).setUp()

        self._path = '/v3io-py-test-emd'

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

        received_items = self._client.new_items_cursor(container=self._container,
                                                       path=self._path + '/',
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

        self._delete_items(self._path, items)

    def test_put_items(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 40, 'feature': 'singing'}
        }

        response = self._client.put_items(container=self._container,
                                          path=self._path, items=items)

        self.assertTrue(response.success)

        self._verify_items(self._path, items)

        self._delete_items(self._path, items)

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
                                   path=path + '/')

    def _verify_items(self, path, items):
        items_cursor = self._client.new_items_cursor(container=self._container,
                                                     path=path + '/',
                                                     attribute_names=['*'])

        received_items = items_cursor.all()

        # TODO: verify contents
        self.assertEqual(len(items), len(received_items))


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
        self._object_dir = '/v3io-py-test-batch-raise'

        # clean up
        self._delete_dir(self._object_dir)

    def test_raise(self):

        def _object_path(idx):
            return self._object_dir + f'/object{idx}'

        def _object_contents(idx):
            return f'object-{idx}'

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
