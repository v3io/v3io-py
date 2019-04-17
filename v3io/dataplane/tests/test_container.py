import os
import unittest
import sys

import future.utils

import v3io.common.helpers
import v3io.dataplane
import v3io.logger


class TestContainer(unittest.TestCase):

    def setUp(self):
        self._logger = v3io.logger.Logger('DEBUG')
        self._logger.set_handler('stdout', sys.stdout, v3io.logger.HumanReadableFormatter())

        # create a context
        context = v3io.dataplane.Context(self._logger, [os.environ['V3IO_DATAPLANE_URL']])

        # create a session and container
        self._container = context.new_session(os.environ['V3IO_DATAPLANE_ACCESS_KEY']).new_container('bigdata')

        self._path = '/emd0'

    def test_emd(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 41, 'feature': 'singing'},
            'louise': {'age': 9, 'feature': 'bunny ears'},
            'tina': {'age': 14, 'feature': 'butts'},
        }

        for item_key, item_attributes in future.utils.viewitems(items):
            response = self._container.put_item(v3io.dataplane.PutItemInput(v3io.common.helpers.url_join(self._path, item_key),
                                                                            item_attributes))

            response.raise_for_status()

        self._verify_items(self._path, items)

        response = self._container.update_item(v3io.dataplane.UpdateItemInput(v3io.common.helpers.url_join(self._path, 'louise'), {
            'height': 130,
            'quip': 'i can smell fear on you'
        }))

        response.raise_for_status()

        response = self._container.get_item(v3io.dataplane.GetItemInput(v3io.common.helpers.url_join(self._path, 'louise'),
                                                                        attribute_names=['__size', 'age', 'quip', 'height']))

        response.raise_for_status()

        self.assertEqual(0, response.output.item['__size'])
        self.assertEqual(9, response.output.item['age'])
        self.assertEqual('i can smell fear on you', response.output.item['quip'])
        self.assertEqual(130, response.output.item['height'])

        get_items_input = v3io.dataplane.GetItemsInput(path=self._path + '/',
                                                       attribute_names=['age', 'feature'],
                                                       filter_expression='age > 15')

        received_items = self._container.new_items_cursor(get_items_input).all()

        self.assertEqual(2, len(received_items))
        for item in received_items:
            self.assertLess(15, item['age'])

        #
        # Increment age
        #

        response = self._container.update_item(v3io.dataplane.UpdateItemInput(v3io.common.helpers.url_join(self._path, 'louise'),
                                                                              expression='age = age + 1'))

        response.raise_for_status()

        response = self._container.get_item(v3io.dataplane.GetItemInput(v3io.common.helpers.url_join(self._path, 'louise'),
                                                                        attribute_names=['age']))

        response.raise_for_status()

        self.assertEqual(10, response.output.item['age'])

        self._delete_items(self._path, items)

    def test_put_items(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 40, 'feature': 'singing'}
        }

        response = self._container.put_items(v3io.dataplane.PutItemsInput(self._path, items))

        self.assertTrue(response.success)

        self._verify_items('/emd0', items)

        self._delete_items(self._path, items)

    def test_put_items_with_error(self):
        items = {
            'bob': {'age': 42, 'feature': 'mustache'},
            'linda': {'age': 40, 'feature': 'singing'},
            'invalid': {'__name': 'foo', 'feature': 'singing'}
        }

        response = self._container.put_items(v3io.dataplane.PutItemsInput(self._path, items))

        self.assertFalse(response.success)

        # first two should've passed
        response.responses[0].raise_for_status()
        response.responses[1].raise_for_status()
        self.assertEqual(403, response.responses[2].status_code)

        # remove invalid so we can verify
        del items['invalid']

        self._verify_items(self._path, items)

    def test_object(self):
        path = '/object.txt'
        contents = 'vegans are better than everyone'

        response = self._container.get_object(v3io.dataplane.GetObjectInput(path))

        self.assertEqual(404, response.status_code)

        # put contents to some object
        response = self._container.put_object(v3io.dataplane.PutObjectInput(path, 0, contents))

        response.raise_for_status()

        # get the contents
        response = self._container.get_object(v3io.dataplane.GetObjectInput(path))

        response.raise_for_status()
        self.assertEqual(response.body, contents)

        # delete the object
        response = self._container.delete_object(v3io.dataplane.DeleteObjectInput(path))

        response.raise_for_status()

        # get again
        response = self._container.get_object(v3io.dataplane.GetObjectInput(path))

        self.assertEqual(404, response.status_code)

    def _delete_items(self, path, items):

        # delete items
        for item_key, _ in future.utils.viewitems(items):
            response = self._container.delete_object(v3io.dataplane.DeleteObjectInput(v3io.common.helpers.url_join(path, item_key)))

            response.raise_for_status()

        # delete dir
        response = self._container.delete_object(v3io.dataplane.DeleteObjectInput(path + '/'))

        response.raise_for_status()

    def _verify_items(self, path, items):
        items_cursor = self._container.new_items_cursor(v3io.dataplane.GetItemsInput(path=path + '/',
                                                                                     attribute_names=['*']))

        received_items = items_cursor.all()

        # TODO: verify contents
        self.assertEqual(len(items), len(received_items))
