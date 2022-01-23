import v3io.dataplane.request
import v3io.dataplane.output
import v3io.dataplane.model
import v3io.dataplane.kv_cursor


class Model(v3io.dataplane.model.Model):

    def __init__(self, client):
        self._client = client
        self._access_key = client._access_key
        self._transport = client._transport

    def head(self,
            container,
            path,
            access_key=None,
            raise_for_status=None,
            transport_actions=None):
        """Retrieves system attributes of object from a container.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object, whose `headers` is populated with system attributes of the object.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_head_object,
                                       locals())

    def get(self,
            container,
            path,
            access_key=None,
            raise_for_status=None,
            transport_actions=None,
            offset=None,
            num_bytes=None):
        """Retrieves an object from a container.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        offset (Optional) : int
            A numeric offset into the object (in bytes). Defaults to 0
        num_bytes (Optional) : int
            Number of bytes to return. By default equal to len(object)-offset

        Return Value
        ----------
        A `Response` object, whose `body` is populated with the body of the object.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_object,
                                       locals())

    def put(self,
            container,
            path,
            access_key=None,
            raise_for_status=None,
            transport_actions=None,
            body=None,
            append=None):
        """Adds a new object to a container, or appends data to an existing object. The option to append data is
        extension to the S3 PUT Object capabilities

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        body (Optional) : str
            The contents of the object
        append (Optional) : bool
            If True, the put appends the data to the end of the object. Defaults to False

        Return Value
        ----------
        A `Response` object
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_put_object,
                                       locals())

    def delete(self, container, path, access_key=None, raise_for_status=None, transport_actions=None):
        """Deletes an object from a container.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path of the object
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.

        Return Value
        ----------
        A `Response` object.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_delete_object,
                                       locals())
