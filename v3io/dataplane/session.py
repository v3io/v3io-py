import v3io.dataplane.container


class Session(object):

    def __init__(self, context, access_key):
        self._context = context
        self._access_key = access_key

    def new_items_cursor(self, container_name, **kwargs):
        return self._context.new_items_cursor(container_name, self._access_key, **kwargs)

    def new_container(self, container_name):
        return v3io.dataplane.container.Container(self, container_name)

    def get_containers(self):
        """
        :return: Response
        """
        return self._context.get_containers(self._access_key)

    def get_container_contents(self, container_name, **kwargs):
        """
        :key path:
        :key get_all_attributes:
        :key directories_only:
        :key limit:
        :key marker:
        :return: Response
        """
        return self._context.get_container_contents(container_name, self._access_key, **kwargs)

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

    def create_stream(self, container_name, **kwargs):
        """
        :key path:
        :key shard_count:
        :key retention_period_hours:
        :return: Response
        """
        return self._context.create_stream(container_name, self._access_key, **kwargs)

    def delete_stream(self, container_name, **kwargs):
        """
        :key path:
        :return: Response
        """
        return self._context.delete_stream(container_name, self._access_key, **kwargs)

    def describe_stream(self, container_name, **kwargs):
        """
        :key path:
        :return: Response
        """
        return self._context.describe_stream(container_name, self._access_key, **kwargs)

    def seek_shard(self, container_name, **kwargs):
        """
        :key path:
        :key seek_type:
        :key starting_sequence_number:
        :key timestamp:
        :return: Response
        """
        return self._context.seek_shard(container_name, self._access_key, **kwargs)

    def put_records(self, container_name, **kwargs):
        """
        :key path:
        :key records:
        :return: Response
        """
        return self._context.put_records(container_name, self._access_key, **kwargs)

    def get_records(self, container_name, **kwargs):
        """
        :key path:
        :key location:
        :key limit:
        :return: Response
        """
        return self._context.get_records(container_name, self._access_key, **kwargs)
