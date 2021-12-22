import ujson
import xml.etree.ElementTree

import v3io.dataplane.transport


class Response(object):

    def __init__(self, output, status_code, headers, body):
        self.status_code = status_code
        self.body = body
        self.headers = headers
        self.output = None

        if output and self.body:
            try:
                parsed_output = ujson.loads(self.body)
            except Exception:
                parsed_output = xml.etree.ElementTree.fromstring(self.body)

            self.output = output(parsed_output)

    def raise_for_status(self, expected_statuses=None):
        if expected_statuses == v3io.dataplane.transport.RaiseForStatus.never:
            return

        # "always" and "none" are equivalent. use the one that's faster to compare against
        if expected_statuses == v3io.dataplane.transport.RaiseForStatus.always:
            expected_statuses = None

        if (expected_statuses is None and self.status_code >= 300) \
                or (expected_statuses and self.status_code not in expected_statuses):
            raise RuntimeError('Request failed with status {0}: {1}'.format(self.status_code, self.body))


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
