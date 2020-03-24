import v3io.dataplane.transport


class Batch(object):

    def __init__(self, client):
        self._client = client
        self._encoded_requests = []
        self._inflight_requests = []
        self._transport_actions = v3io.dataplane.transport.Actions.encode_only
        self._transport = self._client._transport

    def put_object(self, container, path, access_key=None, raise_for_status=None, offset=None, body=None):
        request = self._client.put_object(container,
                                          path,
                                          access_key=access_key,
                                          raise_for_status=raise_for_status,
                                          transport_actions=self._transport_actions,
                                          offset=offset,
                                          body=body)

        self._encoded_requests.append(request)

    def get_object(self, container, path, access_key=None, raise_for_status=None):
        request = self._client.get_object(container,
                                          path,
                                          access_key=access_key,
                                          raise_for_status=raise_for_status,
                                          transport_actions=self._transport_actions)

        self._encoded_requests.append(request)

    def wait(self):
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
            response = self._transport.wait_response(inflight_request)

            # add to responses
            responses.append(response)

            # if there's a pending request, send it on the connection that we just read from
            if self._encoded_requests:

                # send the request
                request = self._transport.send_request(self._encoded_requests.pop(0),
                                                       connection_idx=inflight_request.transport.connection_idx)

                # add to inflight requests
                self._inflight_requests.append(request)

        # todo - raise for, etc
        return responses
