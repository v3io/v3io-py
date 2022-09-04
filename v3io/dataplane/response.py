# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ujson
import xml.etree.ElementTree

import v3io.dataplane.transport


class HttpResponseError(Exception):
    """Exception raised on bad http status"""
    pass


class Response(object):

    def __init__(self, output, status_code, headers, body):
        self.status_code = status_code
        self.body = body
        self.headers = headers
        self._output = output
        self._parsed_output = None

    @property
    def output(self):
        if self._parsed_output:
            return self._parsed_output

        if self._output and self.body:
            try:
                # TODO: It's expensive to always try to parse as JSON first. Better use headers or a heuristic to decide the format.
                try:
                    parsed_output = ujson.loads(self.body)
                except Exception:
                    parsed_output = xml.etree.ElementTree.fromstring(self.body)
            except Exception:
                raise HttpResponseError(f"Failed to parse response with status {self.status_code}, "
                                        f"body {self.body}, headers={self.headers}")

            self._parsed_output = self._output(parsed_output)

            return self._parsed_output

    def raise_for_status(self, expected_statuses=None):
        if expected_statuses == v3io.dataplane.transport.RaiseForStatus.never:
            return

        # "always" and "none" are equivalent. use the one that's faster to compare against
        if expected_statuses == v3io.dataplane.transport.RaiseForStatus.always:
            expected_statuses = None

        if (expected_statuses is None and self.status_code >= 300) \
                or (expected_statuses and self.status_code not in expected_statuses):
            raise HttpResponseError('Request failed with status {0}: {1}'.format(self.status_code, self.body))


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
            raise HttpResponseError('Failed to put items')
