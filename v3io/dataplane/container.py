class Container(object):

    def __init__(self, session, container_name):
        self._session = session
        self._container_name = container_name

    def new_items_cursor(self, request_input):
        return self._session.new_items_cursor(self._container_name, request_input)

    def get_object(self, request_input):
        return self._session.get_object(self._container_name, request_input)

    def put_object(self, request_input):
        return self._session.put_object(self._container_name, request_input)

    def delete_object(self, request_input):
        return self._session.delete_object(self._container_name, request_input)

    def put_item(self, request_input):
        return self._session.put_item(self._container_name, request_input)

    def put_items(self, request_input):
        return self._session.put_items(self._container_name, request_input)

    def update_item(self, request_input):
        return self._session.update_item(self._container_name, request_input)

    def get_item(self, request_input):
        return self._session.get_item(self._container_name, request_input)

    def get_items(self, request_input):
        return self._session.get_items(self._container_name, request_input)
