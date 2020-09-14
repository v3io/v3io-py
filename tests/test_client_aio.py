import asyncio

import unittest
import future.utils

import v3io.dataplane
import v3io.aio.dataplane.client


class Test(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self._client = v3io.aio.dataplane.client.Client(logger_verbosity='DEBUG',
                                                        transport_verbosity='DEBUG')

        self._container = 'bigdata'

    async def _delete_dir(self, path):
        response = await self._client.container.list(container=self._container,
                                                     path=path,
                                                     raise_for_status=v3io.dataplane.RaiseForStatus.never)

        if response.status_code == 404:
            return

        if response.status_code != 200:
            raise RuntimeError(response.body)

        for content in response.output.contents:
            await self._client.object.delete(container=self._container, path=content.key)

        for common_prefixes in response.output.common_prefixes:
            await self._client.object.delete(container=self._container,
                                             path=common_prefixes.prefix)


# class TestObject(aiounittest.AsyncTestCase):
#
#     async def test_object(self):
#         self._client = v3io.aio.dataplane.client.Client(logger_verbosity='DEBUG',
#                                                         transport_verbosity='DEBUG')
#
#         response = await self._client.object.get(container='users',
#                                                  path='/foo',
#                                                  raise_for_status=v3io.dataplane.RaiseForStatus.never)
#         print(response.status_code)
#
#         await self._client.object.put(container='users',
#                                       path='/foo',
#                                       body='this is a test')
#
#         response = await self._client.object.get(container='users',
#                                                  path='/foo')
#
#         print(response.body)
#
#         await self._client.object.delete(container='users',
#                                          path='/foo')
#
#         await self._client.close()
#

class TestKv(Test):

    async def asyncSetUp(self):
        await super().asyncSetUp()

        self._path = 'some_dir/v3io-py-test-emd'

        await self._delete_dir(self._path)

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

        # with limit = 0
        received_items = await self._client.kv.new_cursor(container=self._container,
                                                          table_path=self._path,
                                                          attribute_names=['age', 'feature'],
                                                          filter_expression='age > 15',
                                                          limit=0).all()

        self.assertEqual(0, len(received_items))

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

        await self._client.close()

    async def _verify_items(self, path, items):
        items_cursor = self._client.kv.new_cursor(container=self._container,
                                                  table_path=path,
                                                  attribute_names=['*'])

        received_items = await items_cursor.all()

        # TODO: verify contents
        self.assertEqual(len(items), len(received_items))
