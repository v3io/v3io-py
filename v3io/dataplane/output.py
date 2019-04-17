import future.utils


class Output(object):

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


class GetItemOutput(Output):

    def __init__(self, decoded_body):
        self.item = self._decode_typed_attributes(decoded_body.get('Item', {}))


class GetItemsOutput(Output):

    def __init__(self, decoded_body):
        self.last = decoded_body['LastItemIncluded'] == 'TRUE'
        self.next_marker = decoded_body.get('NextMarker')
        self.items = []

        for item in decoded_body['Items']:
            self.items.append(self._decode_typed_attributes(item))
