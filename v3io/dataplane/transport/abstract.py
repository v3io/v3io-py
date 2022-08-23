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
import os

import v3io.dataplane.request
import v3io.dataplane.transport


class Transport(object):

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None, verbosity=None):
        self._logger = logger
        self._endpoint = self._get_endpoint(endpoint)
        self._timeout = timeout
        self.max_connections = max_connections or 8

        self._set_log_method(verbosity)

    def close(self):
        pass

    def requires_access_key(self):
        return False

    def restart(self):
        pass

    def request(self,
                container,
                access_key,
                raise_for_status,
                transport_actions,
                encoder,
                encoder_args,
                output=None):

        # default to sending/receiving
        transport_actions = transport_actions or v3io.dataplane.transport.Actions.send_and_receive

        # allocate a request
        request = v3io.dataplane.request.Request(container,
                                                 access_key,
                                                 raise_for_status,
                                                 encoder,
                                                 encoder_args,
                                                 output)

        # if all we had to do is encode, return now
        if transport_actions == v3io.dataplane.transport.Actions.encode_only:
            return request

        # send the request
        inflight_request = self.send_request(request)

        # wait for the response
        return self.wait_response(inflight_request)

    def send_request(self, request):
        return request

    @staticmethod
    def _get_endpoint(endpoint):

        if endpoint is None:
            endpoint = os.environ.get('V3IO_API')

            if endpoint is None:
                raise RuntimeError('Endpoints must be passed to context or specified in V3IO_API')

        if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
            endpoint = 'http://' + endpoint

        return endpoint.rstrip('/')

    def _set_log_method(self, verbosity):
        # by default, the log method is null
        log_method = self._log_null

        if verbosity == 'DEBUG':
            log_method = self._log

        setattr(self, 'log', log_method)

    def _log(self, message, *args, **kw_args):
        self._logger.debug_with(message, *args, **kw_args)

    def _log_null(self, message, *args, **kw_args):
        pass
