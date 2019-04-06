import ujson
import future.utils


class Response(object):

    def __init__(self, status_code, headers, body):
        self.status_code = status_code
        self.body = body
        self.headers = headers
        self.json = None

        if self.headers.get('Content-Type') == 'application/json' and self.body:
            self.json = ujson.loads(self.body)

    def raise_for_status(self, expected_statuses=None):
        if (expected_statuses is None and self.status_code >= 300) \
                or (expected_statuses and self.status_code not in expected_statuses):
            raise RuntimeError('Request failed with status {0}'.format(self.status_code))

    def _decode_typed_attributes(self, typed_attributes):
        decoded_attributes = {}

        for attribute_key, typed_attribute_value in future.utils.viewitems(typed_attributes):
            for attribute_type, attribute_value in future.utils.viewitems(typed_attribute_value):
                if attribute_type == 'N':
                    decoded_attribute = float(attribute_value)
                elif attribute_type == 'B':
                    decoded_attribute = bytearray(attribute_value)
                else:
                    decoded_attribute = attribute_value

                decoded_attributes[attribute_key] = decoded_attribute

        return decoded_attributes


class GetItemResponse(Response):

    def __init__(self, status_code, headers, body):
        super(GetItemResponse, self).__init__(status_code, headers, body)

        self.item = self._decode_typed_attributes(self.json.get('Item', {}))


class GetItemsResponse(Response):

    def __init__(self, status_code, headers, body):
        super(GetItemsResponse, self).__init__(status_code, headers, body)
        self.last = self.json['LastItemIncluded'] == 'TRUE'
        self.next_marker = self.json.get('NextMarker')
        self.items = []

        for item in self.json['Items']:
            self.items.append(self._decode_typed_attributes(item))


class PutItemsResponse(object):

    def __init__(self):
        self.responses = []
        self.success = True

    def add_response(self, response):
        self.responses.append(response)

        if response.status_code != 200:
            self.success = False

    def raise_for_status(self):
        if not self.success:
            raise RuntimeError('Failed to put items')
