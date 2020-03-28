import functools

import v3io.dataplane.transport


class Batch(object):

    def __init__(self, client):
        self._client = client
        self._encoded_requests = []
        self._inflight_requests = []
        self._transport_actions = v3io.dataplane.transport.Actions.encode_only
        self._transport = self._client._transport

        for client_call in [
            'get_containers',
            'get_container_contents',
            'get_object',
            'put_object',
            'delete_object',
            'put_item',
            'update_item',
            'get_item',
            'get_items',
            'create_stream',
            'delete_stream',
            'describe_stream',
            'seek_shard',
            'put_records',
            'get_records'
        ]:
            setattr(self, client_call, functools.partial(self._call_client, client_call))

    def _call_client(self, name, *args, **kw_args):
        kw_args['transport_actions'] = self._transport_actions
        request = getattr(self._client, name)(*args, **kw_args)

        self._encoded_requests.append(request)

    def wait(self, raise_for_status=None):
        try:
            return self._wait(raise_for_status)

        # if an exception is raised, clean up everything
        except Exception as e:
            self._inflight_requests = []
            self._encoded_requests = []
            self._transport.restart()

            raise e

    def _wait(self, raise_for_status=None):

        responses = []

        # while we can send requests - send them
        while self._encoded_requests and len(self._inflight_requests) < self._transport.max_connections:

            # send the request
            request = self._transport.send_request(self._encoded_requests.pop(0))

            # add to inflight requests
            self._inflight_requests.append(request)

        # start creating responses
        while self._inflight_requests:

            # get an inflight request
            inflight_request = self._inflight_requests.pop(0)

            # wait for the response of the request
            response = self._transport.wait_response(inflight_request, raise_for_status)

            # add to responses
            responses.append(response)

            # if there's a pending request, send it on the connection that we just read from
            if self._encoded_requests:

                # send the request
                request = self._transport.send_request(self._encoded_requests.pop(0),
                                                       transport_state=inflight_request.transport)

                # add to inflight requests
                self._inflight_requests.append(request)

        return responses
