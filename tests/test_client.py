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
        self._client = v3io.dataplane.Client(self._logger)

        self._container = 'bigdata'

    def tearDown(self):
        self._client.close()

    def _delete_dir(self, path):
        response = self._client.get_container_contents(container=self._container,
                                                       path=path,
                                                       raise_for_status=v3io.dataplane.RaiseForStatus.never)

        if response.status_code == 404:
            return

        for content in response.output.contents:
            self._client.delete_object(container=self._container, path=content.key)

        for common_prefixes in response.output.common_prefixes:
            self._client.delete_object(container=self._container,
                                       path=common_prefixes.prefix)


class TestContainer(Test):

    def setUp(self):
        super(TestContainer, self).setUp()
        self._path = '/v3io-py-test-container'

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

        self._path = '/v3io-py-test-stream/'

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

        self._path = '/v3io-py-test-object/object.txt'

        # clean up
        self._delete_dir('/v3io-py-test-object')

    def test_object(self):
        contents = 'vegans are better than everyone'

        response = self._client.get_object(container=self._container,
                                           path=self._path,
                                           raise_for_status=v3io.dataplane.RaiseForStatus.never)

        self.assertEqual(404, response.status_code)

        # put contents to some object
        self._client.put_object(container=self._container,
                                path=self._path,
                                offset=0,
                                body=contents)

        # get the contents
        response = self._client.get_object(container=self._container,
                                           path=self._path)

        if not isinstance(response.body, str):
            response.body = response.body.decode('utf-8')

        self.assertEqual(response.body, contents)

        # delete the object
        self._client.delete_object(container=self._container,
                                   path=self._path)

        # get again
        response = self._client.get_object(container=self._container,
                                           path=self._path,
                                           raise_for_status=v3io.dataplane.RaiseForStatus.never)

        self.assertEqual(404, response.status_code)


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
