class Container(object):

    def __init__(self, session, container_name):
        self._session = session
        self._container_name = container_name

    def new_items_cursor(self, **kwargs):
        """
        :key path:
        :key attribute_names:
        :key filter_expression:
        :return: Response
        """
        return self._session.new_items_cursor(self._container_name, **kwargs)

    def get_container_contents(self, **kwargs):
        """
        :key path:
        :key get_all_attributes:
        :key directories_only:
        :key limit:
        :key marker:
        :return: Response
        """
        return self._session.get_container_contents(self._container_name, **kwargs)

    def get_object(self, **kwargs):
        """
        :key path:
        :key offset:
        :key num_bytes:
        :return: Response
        """
        return self._session.get_object(self._container_name, **kwargs)

    def put_object(self, **kwargs):
        """
        :key path:
        :key offset:
        :key body:
        :return: Response
        """
        return self._session.put_object(self._container_name, **kwargs)

    def delete_object(self, **kwargs):
        """
        :key path:
        :return: Response
        """
        return self._session.delete_object(self._container_name, **kwargs)

    def put_item(self, **kwargs):
        """
        :key path:
        :key attributes:
        :key condition:
        :key update_mode:
        :return: Response
        """
        return self._session.put_item(self._container_name, **kwargs)

    def put_items(self, **kwargs):
        """
        :key path:
        :key items:
        :key condition:
        :return: Response
        """
        return self._session.put_items(self._container_name, **kwargs)

    def update_item(self, **kwargs):
        """
        :key path:
        :key attributes:
        :key expression:
        :key condition:
        :key update_mode:
        :return: Response
        """
        return self._session.update_item(self._container_name, **kwargs)

    def get_item(self, **kwargs):
        """
        :key path:
        :key attribute_names:
        :return: Response
        """
        return self._session.get_item(self._container_name, **kwargs)

    def get_items(self, **kwargs):
        """
        :key path:
        :key table_name:
        :key attribute_names:
        :key filter:
        :key marker:
        :key sharding_key:
        :key limit:
        :key segment:
        :key total_segments:
        :key sort_key_range_start:
        :key sort_key_range_end:
        :return: Response
        """
        return self._session.get_items(self._container_name, **kwargs)

    def create_stream(self, **kwargs):
        """
        :key path:
        :key shard_count:
        :key retention_period_hours:
        :return: Response
        """
        return self._session.create_stream(self._container_name, **kwargs)

    def delete_stream(self, **kwargs):
        """
        :key path:
        :return: Response
        """
        return self._session.delete_stream(self._container_name, **kwargs)

    def describe_stream(self, **kwargs):
        """
        :key path:
        :return: Response
        """
        return self._session.describe_stream(self._container_name, **kwargs)

    def seek_shard(self, **kwargs):
        """
        :key path:
        :key seek_type:
        :key starting_sequence_number:
        :key timestamp:
        :return: Response
        """
        return self._session.seek_shard(self._container_name, **kwargs)

    def put_records(self, **kwargs):
        """
        :key path:
        :key records:
        :return: Response
        """
        return self._session.put_records(self._container_name, **kwargs)

    def get_records(self, **kwargs):
        """
        :key path:
        :key location:
        :key limit:
        :return: Response
        """
        return self._session.get_records(self._container_name, **kwargs)