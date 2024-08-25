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
import xml.etree.ElementTree

import orjson

import v3io.dataplane.transport


class HttpResponseError(Exception):
    """Exception raised on bad http status"""

    def __init__(self, message="", status_code=None):
        super().__init__(message)
        self.status_code = status_code

    def __repr__(self):
        return f"HttpResponseError('{self}', {self.status_code})"


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
                # TODO: It's expensive to always try to parse as JSON first. Better
                #       use headers or a heuristic to decide the format.
                try:
                    parsed_output = orjson.loads(self.body)
                except Exception:
                    parsed_output = xml.etree.ElementTree.fromstring(self.body)
            except Exception:
                raise HttpResponseError(
                    f"Failed to parse response with status {self.status_code}, "
                    f"body {self.body}, headers={self.headers}",
                    self.status_code,
                )

            self._parsed_output = self._output(parsed_output)

            return self._parsed_output

    def raise_for_status(self, expected_statuses=None):
        if expected_statuses == v3io.dataplane.transport.RaiseForStatus.never:
            return

        # "always" and "none" are equivalent. use the one that's faster to compare against
        if expected_statuses == v3io.dataplane.transport.RaiseForStatus.always:
            expected_statuses = None

        if (expected_statuses is None and self.status_code >= 300) or (
            expected_statuses and self.status_code not in expected_statuses
        ):
            if 308 <= self.status_code < 400:
                location = self.headers.get("Location")
                error_message = "Request failed due to a Permanent Redirect."
                if location:
                    error_message += f" Please change URL to: {location}"
            else:
                error_message = f"Request failed with status {self.status_code}: {self.body}"
            raise HttpResponseError(error_message, status_code=self.status_code)


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
            raise HttpResponseError("Failed to put items")
