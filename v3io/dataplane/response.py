import ujson


class Response(object):

    def __init__(self, output, status_code, headers, body):
        self.status_code = status_code
        self.body = body
        self.headers = headers
        self.output = None

        if output and self.headers.get('Content-Type') == 'application/json' and self.body:
            self.output = output(ujson.loads(self.body))

    def raise_for_status(self, expected_statuses=None):
        if (expected_statuses is None and self.status_code >= 300) \
                or (expected_statuses and self.status_code not in expected_statuses):
            raise RuntimeError('Request failed with status {0}'.format(self.status_code))


class Responses(object):

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
