import v3io.dataplane.request
import v3io.dataplane.output
import v3io.dataplane.model
import v3io.dataplane.kv_cursor


class Model(v3io.dataplane.model.Model):

    def __init__(self, client):
        self._client = client
        self._access_key = client._access_key
        self._transport = client._transport

    def list(self,
             container,
             path,
             access_key=None,
             raise_for_status=None,
             transport_actions=None,
             get_all_attributes=None,
             directories_only=None,
             limit=None,
             marker=None):
        """Lists the containers contents.

        Parameters
        ----------
        container (Required) : str
            The container on which to operate.
        path (Required) : str
            The path within the container
        access_key (Optional) : str
            The access key with which to authenticate. Defaults to the V3IO_ACCESS_KEY env.
        get_all_attributes (Optional) : bool
            False (default) - retrieves basic attributes
            True - retrieves all attributes of the underlying objects
        directories_only (Optional) : bool
            False (default) - retrieves objects (contents) and directories (common prefixes)
            True - retrieves only directories (common prefixes)
        limit (Optional) : int
            Number of objects/directories to receive. default: 1000
        marker (Optional) : str
            An opaque identifier that was returned in the NextMarker element of a response to a previous
            get_container_contents request that did not return all the requested items. This marker identifies the
            location in the path from which to start searching for the remaining requested items.

        Return Value
        ----------
        A `Response` object, whose `output` is `GetContainerContentsOutput`.
        """
        return self._transport.request(container,
                                       access_key or self._access_key,
                                       raise_for_status,
                                       transport_actions,
                                       v3io.dataplane.request.encode_get_container_contents,
                                       locals(),
                                       v3io.dataplane.output.GetContainerContentsOutput)
