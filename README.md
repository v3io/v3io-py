# Python SDK for V3IO

Python (2.7 and 3.5+) client for the Iguazio Data Science Platform (the "platform"). Designed to allow fast access to the data layer and basic access to the control layer. This library can be used in Nuclio functions, Jupyter notebooks, local Python IDEs; anywhere with a Python interpreter and access to a platform. 

# Installing
Simply get with `pip` (locking to a specific version is recommended, as always):
```
pip install v3io
```

# Dataplane client
With the dataplane client you can manipulate data in the platform's multi-model data layer, including:
* Objects
* Key-values (NoSQL)
* Streams
* Containers

Under the hood, the client connects through the platform's web API (https://www.iguazio.com/docs/reference/latest-release/api-reference/web-apis/) and wraps each low level API with an interface. Calls are blocking, but you can use the batching interface to send multiple requests in parallel for greater performance. 

## Creating a client
Create a dataplane client, passing in the web API endpoint and your access key:

```python
import v3io.dataplane


v3io_client = v3io.dataplane.Client(endpoint='https://v3io-webapi:8081', access_key='some_access_key')
```

> Note: In some environments (like Jupyter notebooks) you do not need to pass the endpoint and access key, as they will automatically be inferred from the environment

### Making requests
The client supports a handful of low level APIs, each receiving different arguments but all returning a `Response` object. A `Response` object holds 4 fields:
* `status_code`: The HTTP status code returned
* `output`: An object containing the parsed response (each API returns a different object)
* `headers` and `body`: The raw headers and body of the response

You would normally only access the `output` field unless an API was called that returns raw data like `get_object` (in which case `body` holds the response). Consult the reference for each API call to see how to handle its `Response` object. In the example below, we perform a simple request to get the containers available in our tenant, print the returned status code and containers:

```python
# list the containers we can access
response = v3io_client.get_containers()

# print the status code. outputs:
# 
#  Status code: 200
#
print(f'Status code: {response.status_code}')

# iterate over the containers and print them
# 
#   #0: bigdata
#   #1: users
#
for container_idx, container in enumerate(response.output.containers):
    print(f'#{container_idx}: {container.name}')
```

We can also get help information about the parameters this API call receives:
```python
help(v3io_client.get_containers)
```

### Handling errors
By default, making a request will raise an exception if any non-200 status code is returned. We can override this default behavior in two ways. 

The first is to simply never raise an exception and handle the status manually:
```python
# list the containers we can access and never raise an exception
response = v3io_client.get_containers(raise_for_status=v3io.dataplane.RaiseForStatus.never)

# do anything we want with the status code
# some_logic(response.status_code)
```

The second is to indicate which status codes are acceptable:
```python
# list the containers and raise if the status code is not 200 or 204
response = v3io_client.get_containers(raise_for_status=[200, 204])
```

### Creating batches
To get the highest possible throughput, we can send many requests towards the data layer and wait for all the responses to arrive (rather than send each request and wait for the response). The SDK supports this through batching. Any API call can be made through the client's built in `batch` object. The API call receives the exact same arguments it would normally receive (except for `raise_for_status`), and does not block until the response arrives. To wait for all pending responses, call `wait()` on the `batch` object:

```python
# do 16 writes in parallel
for idx in range(16):

    # returns immediately
    v3io_client.batch.put_object(container='bigdata',
                                 path=f'/object{idx}',
                                 body=f'object-{idx}')

# wait for all writes to complete
v3io_client.batch.wait()
```

The looped `put_object` interface above will send 16 `put object` requests to the data layer in parallel. When `wait` is called, it will block until either all responses arrive (in which case it will return a `Responses` object, containing the `responses` of each call) or an error occurs - in which case an exception is thrown. You can pass `raise_for_status` to `wait`, and it behaves as explained above.

> Note: The `batch` object is stateful, so you can only create one batch at a time. However, you can create multiple parallel batches yourself through the client's `create_batch()` interface

## Examples

### Accessing objects

Put data in an object, get it back and then delete the object:

```python
# put contents to some object
v3io_client.put_object(container='users',
                       path='/my-object',
                       body='hello, there')

# get the object
response = v3io_client.get_object(container='users', path='/my-object')

# print the contents. outputs:
#
#   hello, there
#
print(response.body.decode('utf-8'))

# delete the object
v3io_client.delete_object(container='users', path='/my-object')
```

### Accessing key-values (NoSQL)
Create a table, update a record and run a query.

```python
items = {
    'bob': {'age': 42, 'feature': 'mustache'},
    'linda': {'age': 41, 'feature': 'singing'},
    'louise': {'age': 9, 'feature': 'bunny ears'},
}

# add the records to the table
for item_key, item_attributes in items.items():
    v3io_client.put_item(container='users', path='/bobs-burgers/' + item_key, attributes=item_attributes)

# adds two fields (height, quip) to the louise record
v3io_client.update_item(container='users',
                        path='/bobs-burgers/louise',
                        attributes={
                            'height': 130,
                            'quip': 'i can smell fear on you'
                        })

# get a record by key, specifying specific arguments
response = v3io_client.get_item(container='users', 
                                path='/bobs-burgers/louise', 
                                attribute_names=['__size', 'age', 'quip', 'height'])


# print the item from the response. outputs:
#
#   {'__size': 0.0, 'quip': 'i can smell fear on you', 'height': 130.0}
#
print(response.output.item)

# create a query, and use an items cursor to iterate the results
items_cursor = v3io_client.new_items_cursor(container='users',
                                            path='/bobs-burgers/',
                                            attribute_names=['age', 'feature'],
                                            filter_expression='age > 15')

# print the output
for item in items_cursor.all():
    print(item)
```

### Accessing streams
Creates a stream with several partitions, writes records to it, reads the records and deletes the stream:

```python
# create a stream w/8 shards
v3io_client.create_stream(container='users',
                           path='/my-test-stream',
                           shard_count=8)

# write 4 records - 3 with explicitly specifying the shard and 1 using hashing
records = [
    {'shard_id': 1, 'data': 'first shard record #1'},
    {'shard_id': 1, 'data': 'first shard record #2'},
    {'shard_id': 2, 'data': 'second shard record #1'},
    {'data': 'some shard record #1'}
]

v3io_client.put_records(container='users', path='/my-test-stream', records=records)

# seek to the beginning of the shard of #1 so we know where to read from 
response = v3io_client.seek_shard(container='users', path='/my-test-stream/1', seek_type='EARLIEST')

# get records from the shard (should receive 2)
response = v3io_client.get_records(container='users', path='/my-test-stream/1', location=response.output.location)

# print the records. outputs:
#
#   first shard record #1
#   first shard record #2
#
for record in response.output.records:
    print(record.data.decode('utf-8'))

# delete the stream
v3io_client.delete_stream(container='users', path='/my-test-stream')
```

# Controlplane client
Coming soon.