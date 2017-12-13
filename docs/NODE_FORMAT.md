## Corfu Node IDs

This document describes the format of Corfu node identifiers (node IDs), which are used to refer to unique Corfu instances.
Node Ids provide an extra layer of reliability in the case of a server or network reconfiguration. For example, a DNS error
could point a Corfu client at an incorrect node. This could lead a client to read an update from the wrong system, or believe
that it has committed an update when it is not committed. Node IDs prevent this issue by assigning each node a unique 128-bit
identifier at startup. When clients connect to a Corfu node, they verify that the correct Node ID is presented by the node
before interacting with it.

Node IDs consist of the following components:

- ``<protocol>``: The protocol that used by the node. Currently, only ``tcp`` is supported.
- ``<host-name>``: The host name the node can be reached at. This can be a DNS name, an IPv4 address or an IPv6 address.
If an IPv6 address is provided, it must be delimited by brackets (``[<ipv6-host-name>]``).
- ``<port>``: The port the node is providing the Corfu service on.
- ``<id>``: A 128-bit unique identifier for the node. This parameter is optional, andd if not provided, the identifier will not
be checked at connection time.
- ``<options>``: A list of options required when connecting to the node, in ``<option-name>=<option-value>`` format. Used for
settings such as TLS.

Node IDs may be provided in a human readable format. The format of a human-readable node-id is as follows:

(``<protocol>``://)``<host-name>``:``<port>``(/``<id>``)(?``<options>``)

If (``<protocol>``://) is omitted, it is assumed that the protocol is ``tcp``.

If (/``<id>``) is omitted, no identifier checking will be done.

If (?``<options>``) are omittedd, no options will be usedd to connect to the server instance. 


``<id>`` is in base64 url-safe (RFC 4648) 

For example, a node using the ``tcp`` protocol at ``10.0.0.1`` on port ``9000`` with node id ``fZPF5eGIScaq9m1DabhaCQ`` and no
options would use the string:
``tcp://10.0.0.1:9000/fZPF5eGIScaq9m1DabhaCQ``
