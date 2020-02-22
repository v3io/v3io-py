import v3io.dataplane.container


class Session(object):

    def __init__(self, context, transport, access_key):
        self._context = context
        self._transport = transport
        self._access_key = access_key

    def new_container(self, container_name):
        return v3io.dataplane.container.Container(self._context, self._access_key, self._transport, container_name)
