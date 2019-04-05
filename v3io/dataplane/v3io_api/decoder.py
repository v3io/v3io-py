import ujson

import response


class ResponseDecoder(object):

    def __init__(self):
        pass

    def decode_response(self, status_code, headers, body):
        return response.Response(status_code, headers, body)

    def decode_get_item(self, status_code, headers, body):
        return response.GetItemResponse(status_code, headers, body)

    def decode_get_items(self, status_code, headers, body):
        return response.GetItemsResponse(status_code, headers, body)
