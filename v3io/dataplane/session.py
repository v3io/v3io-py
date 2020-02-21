import v3io.dataplane.container


class Session(object):

    def __init__(self, context, access_key):
        self._context = context
        self._access_key = access_key

    def new_items_cursor(self, container_name, **kwargs):
        return self._context.new_items_cursor(container_name, self._access_key, **kwargs)

    def new_container(self, container_name):
        return v3io.dataplane.container.Container(self, container_name)

    def get_object(self, container_name, **kwargs):
        return self._context.get_object(container_name, self._access_key, **kwargs)

    def put_object(self, container_name, **kwargs):
        return self._context.put_object(container_name, self._access_key, **kwargs)

    def delete_object(self, container_name, **kwargs):
        return self._context.delete_object(container_name, self._access_key, **kwargs)

    def put_item(self, container_name, **kwargs):
        return self._context.put_item(container_name, self._access_key, **kwargs)

    def put_items(self, container_name, **kwargs):
        return self._context.put_items(container_name, self._access_key, **kwargs)

    def update_item(self, container_name, **kwargs):
        return self._context.update_item(container_name, self._access_key, **kwargs)

    def get_item(self, container_name, **kwargs):
        return self._context.get_item(container_name, self._access_key, **kwargs)

    def get_items(self, container_name, **kwargs):
        return self._context.get_items(container_name, self._access_key, **kwargs)
