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

    def get_object(self, **kwargs):
        """
        :key path:
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
        :key attribute_names:
        :key filter_expression:
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
