class ItemsCursor(object):

    def __init__(self,
                 context,
                 container_name,
                 access_key,
                 request_input):
        self._context = context
        self._current_response = None
        self._current_items = None
        self._current_item = None
        self._current_item_index = 0

        # get items params
        self._container_name = container_name
        self._access_key = access_key
        self._request_input = request_input

    def next_item(self):
        if self._current_item_index < len(self._current_items or []):
            self._current_item = self._current_items[self._current_item_index]
            self._current_item_index += 1

            return self._current_item

        if self._current_response and self._current_response.output.last:
            return None

        self._request_input.marker = self._current_response.output.next_marker if self._current_response else None

        # get the next batch
        self._current_response = self._context.get_items(self._container_name,
                                                         self._access_key,
                                                         self._request_input)

        # raise if there was an issue
        self._current_response.raise_for_status()

        # set items
        self._current_items = self._current_response.output.items
        self._current_item_index = 0

        # and recurse into next now that we repopulated response
        return self.next_item()

    def all(self):
        items = []

        while True:
            item = self.next_item()

            if item is None:
                break

            items.append(item)

        return items
