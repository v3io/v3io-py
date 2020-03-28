import os

import v3io.dataplane.request
import v3io.dataplane.transport


class Transport(object):

    def __init__(self, logger, endpoint=None, max_connections=None, timeout=None):
        self._logger = logger
        self._endpoint = self._get_endpoint(endpoint)
        self._timeout = timeout
        self.max_connections = max_connections or 8

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

    @staticmethod
    def _get_endpoint(endpoint):

        if endpoint is None:
            endpoint = os.environ.get('V3IO_API')

            if endpoint is None:
                raise RuntimeError('Endpoints must be passed to context or specified in V3IO_API')

        if not endpoint.startswith('http://') and not endpoint.startswith('https://'):
            endpoint = 'http://' + endpoint

        return endpoint.rstrip('/')
