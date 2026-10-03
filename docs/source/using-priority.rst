Using Priority
==============

Priority has a simple API. Streams are inserted into the tree: when they are
inserted, they may optionally have a weight, depend on another stream, or
become an exclusive dependent of another stream. To manipulate the tree, we
use a :class:`PriorityTree <priority.PriorityTree>` object.

.. code-block:: python

    >>> p = priority.PriorityTree()
    >>> p.insert_stream(stream_id=1)
    >>> p.insert_stream(stream_id=3)
    >>> p.insert_stream(stream_id=5, depends_on=1)
    >>> p.insert_stream(stream_id=7, weight=32)
    >>> p.insert_stream(stream_id=9, depends_on=7, weight=8)
    >>> p.insert_stream(stream_id=11, depends_on=7, exclusive=True)

Once streams are inserted, the stream priorities can be requested. This allows
the server to make decisions about how to allocate resources.

Iterating The Tree
------------------

The tree in this algorithm acts as a gate. Its goal is to allow one stream
"through" at a time, in such a manner that all the active streams are served as
evenly as possible in proportion to their weights.

This is handled in Priority by iterating over the tree. The tree itself is an
iterator, and each time it is advanced it will yield a stream ID. This is the
ID of the stream that should next send data.

This looks like this:

.. code-block:: python

    >>> for stream_id in p:
    ...     send_data(stream_id)

If each stream only sends when it is 'ungated' by this mechanism, the server
will automatically be emitting stream data in conformance to RFC 7540.

Updating The Tree
-----------------

If for any reason a stream is unable to proceed (for example, it is blocked on
HTTP/2 flow control, or it is waiting for more data from another service), that
stream is *blocked*. The :class:`PriorityTree <priority.PriorityTree>` should
be informed that the stream is blocked so that other dependent streams get a
chance to proceed. This can be done by calling the
:meth:`block <priority.PriorityTree.block>` method of the tree with the stream
ID that is currently unable to proceed. This will automatically update the
tree, and it will adjust on the fly to correctly allow any streams that were
dependent on the blocked one to progress.

For example:

.. code-block:: python

    >>> for stream_id in p:
    ...     send_data(stream_id)
    ...     if blocked(stream_id):
    ...         p.block(stream_id)

When a stream goes from being blocked to being unblocked, call the
:meth:`unblock <priority.PriorityTree.unblock>` method to place it back into
the sequence. Both the :meth:`block <priority.PriorityTree.block>` and
:meth:`unblock <priority.PriorityTree.unblock>` methods are idempotent and safe
to call repeatedly.

Additionally, the priority of a stream may change. When it does, the
:meth:`reprioritize <priority.PriorityTree.reprioritize>` method can be used to
update the tree in the wake of that change.
:meth:`reprioritize <priority.PriorityTree.reprioritize>` has the same
signature as :meth:`insert_stream <priority.PriorityTree.insert_stream>`, but
applies only to streams already in the tree.

Placeholder Parents
-------------------

A dependency may refer to a stream that has not been registered yet. Priority
represents that reference with a blocked *placeholder* node rather than treating
it as a registered stream. Placeholders are tracked separately and are subject
to their own ``maximum_placeholders`` budget. A stream that was explicitly
inserted and later blocked is never selected as a placeholder, even though it is
temporarily inactive.

When a stream moves away from a childless placeholder, the placeholder no longer
represents any dependency and is removed immediately. If the placeholder budget
is exhausted while creating a new parent, Priority compresses one blocked
placeholder instead. Childless placeholders are preferred; if every placeholder
has children, the blocked placeholder with the lowest stream ID is flattened.

Compressing a placeholder moves its children to the placeholder's parent. Let
the placeholder's scheduling weight be ``W`` and its children's current
effective weights be ``w_1`` ... ``w_n``. Each child receives the effective
weight

.. code-block:: text

    w_i * W / (w_1 + ... + w_n)

This preserves both the children's aggregate parent-level share and their
relative shares. The stream's declared HTTP/2 weight remains an integer between
1 and 256; the possibly fractional value is used only by the internal scheduler.
Active streams remain reachable through the same active-or-blocked ancestor gate
after compression.

Registering a stream whose ID matches an uncompressed placeholder turns that
node into a normal stream and then applies the requested weight, dependency and
exclusive flags. If a placeholder was compressed, registering the same ID later
creates a new normal stream and does not restore the dependencies that were
reparented during compression. Ordinary and exclusive reprioritization continue
to work after either outcome.

Removing Streams
----------------

A stream can be entirely removed from the tree by calling
:meth:`remove_stream <priority.PriorityTree.remove_stream>`. Note that this is
*not* idempotent. Further, calling
:meth:`remove_stream <priority.PriorityTree.remove_stream>` and then re-adding
it *may* cause a substantial change in the shape of the priority tree, and
*will* cause the iteration order to change.
