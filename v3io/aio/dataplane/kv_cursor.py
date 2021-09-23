class Cursor(object):

    def __init__(self,
                 context,
                 container_name,
                 access_key,
                 table_path,
                 raise_for_status=None,
                 attribute_names='*',
                 filter_expression=None,
                 marker=None,
                 sharding_key=None,
                 limit=None,
                 segment=None,
                 total_segments=None,
                 sort_key_range_start=None,
                 sort_key_range_end=None):
        self._context = context
        self._container_name = container_name
        self._access_key = access_key
        self._current_response = None
        self._current_items = None
        self._current_item = None
        self._current_item_index = 0
        self._total_items_read = 0

        # get items params
        self.raise_for_status = raise_for_status
        self.table_path = table_path
        self.attribute_names = attribute_names
        self.filter_expression = filter_expression
        self.marker = marker
        self.sharding_key = sharding_key
        self.limit = limit
        self.segment = segment
        self.total_segments = total_segments
        self.sort_key_range_start = sort_key_range_start
        self.sort_key_range_end = sort_key_range_end

    async def next_item(self):
        calculated_limit = self.limit

        # check if we already reached the limit we were asked for
        if self.limit is not None:

            # if we already passed the limit, stop here
            if self._total_items_read >= self.limit:
                return None

            # don't ask for more items than we'll read
            calculated_limit -= self._total_items_read

        if self._current_item_index < len(self._current_items or []):
            self._current_item = self._current_items[self._current_item_index]
            self._current_item_index += 1
            self._total_items_read += 1

            return self._current_item

        if self._current_response and (self._current_response.output.last or len(self._current_items) == 0):
            return None

        self.marker = self._current_response.output.next_marker if self._current_response else None

        # get the next batch
        self._current_response = await self._context.kv.scan(self._container_name,
                                                             self.table_path,
                                                             self._access_key,
                                                             self.raise_for_status,
                                                             self.attribute_names,
                                                             self.filter_expression,
                                                             self.marker,
                                                             self.sharding_key,
                                                             calculated_limit,
                                                             self.segment,
                                                             self.total_segments,
                                                             self.sort_key_range_start,
                                                             self.sort_key_range_end)

        # raise if there was an issue
        self._current_response.raise_for_status(self.raise_for_status)

        # set items
        self._current_items = self._current_response.output.items
        self._current_item_index = 0

        # and recurse into next now that we repopulated response
        return await self.next_item()

    async def all(self):
        items = []

        while True:
            item = await self.next_item()

            if item is None:
                break

            items.append(item)

        return items
