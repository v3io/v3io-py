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
from . import abstract


class Transport(abstract.Transport):

    def __init__(self, request_verifiers):
        super().__init__(None, '', 0, None, 'DEBUG')
        self._request_verifiers = request_verifiers
        self._current_request_index = 0

    def close(self):
        pass

    def wait_response(self, request, raise_for_status=None):
        if self._current_request_index > len(self._request_verifiers):
            raise IndexError(f'Have only {len(self._request_verifiers)} verifiers, got {self._current_request_index} requests')

        # call the verifier, get the response
        response = self._request_verifiers[self._current_request_index](request)

        self._current_request_index += 1

        return response
