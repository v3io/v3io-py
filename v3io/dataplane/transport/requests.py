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
import requests

import v3io.dataplane.request
import v3io.dataplane.response

from . import abstract


class Transport(abstract.Transport):
    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None, verbosity=None):
        super(Transport, self).__init__(logger, endpoint, max_connections, timeout, verbosity)
        self._next_connection_pool = 0
        self._session = requests.Session()

    def requires_access_key(self):
        return True

    def close(self):
        self._session.close()

    def send_request(self, request):
        # call the encoder to get the response
        http_response = self._http_request(request.method, request.path, request.headers, request.body)

        # set http response
        request.transport.http_response = http_response

        return request

    def wait_response(self, request, raise_for_status=None, num_retries=1):

        # create a response
        response = v3io.dataplane.response.Response(
            request.output,
            request.transport.http_response.status_code,
            request.headers,
            request.transport.http_response.content,
        )

        # enforce raise for status
        response.raise_for_status(request.raise_for_status or raise_for_status)

        return response

    def _http_request(self, method, path, headers=None, body=None):
        self.log("Tx", method=method, path=path, headers=headers, body=body)

        response = self._session.request(
            method, self._endpoint + path, headers=headers, data=body, timeout=self._timeout, verify=False
        )

        self.log("Rx", status_code=response.status_code, headers=response.headers, body=response.text)

        return response
