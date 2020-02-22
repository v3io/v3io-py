import os

import v3io.dataplane.transport
import v3io.dataplane.session
import v3io.dataplane.request


class Context(object):

    def __init__(self, logger, endpoints=None, max_connections=4, timeout=None):
        self._logger = logger
        self._transport = v3io.dataplane.transport.Transport(logger, endpoints, max_connections, timeout)
        self._access_key = os.environ['V3IO_ACCESS_KEY']

    def new_session(self, access_key=None):
        return v3io.dataplane.session.Session(self,
                                              self._transport,
                                              access_key or self._access_key)

    def delete_object(self, container_name, path, access_key=None):
        return self._transport.encode_and_send(container_name,
                                               access_key,
                                               v3io.dataplane.request.encode_delete_object,
                                               locals())
