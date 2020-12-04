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
