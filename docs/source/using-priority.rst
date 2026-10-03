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

Pausing Subtrees
----------------

When an upstream connection stops sending, every logical stream below its
current priority node must stop temporarily. Call
:meth:`pause_subtree <priority.PriorityTree.pause_subtree>` to get a
:class:`SubtreePauseHandle <priority.SubtreePauseHandle>`. The paused stream
and all of its descendants are removed from scheduling, but their individual
:meth:`block <priority.PriorityTree.block>` states are preserved.

Pauses may be nested. Each manager must release its own handle, and a subtree
is schedulable again only after all of its handles have been released. Streams
that move into or out of the subtree while it is paused are scheduled
according to their current ancestors. Removing the paused stream closes all
handles for that stream and prevents them from affecting streams that are
reparented when it is removed.

For example:

.. code-block:: python

    >>> pause = p.pause_subtree(stream_id)
    >>> # ...
    >>> pause.release()

Additionally, the priority of a stream may change. When it does, the
:meth:`reprioritize <priority.PriorityTree.reprioritize>` method can be used to
update the tree in the wake of that change.
:meth:`reprioritize <priority.PriorityTree.reprioritize>` has the same
signature as :meth:`insert_stream <priority.PriorityTree.insert_stream>`, but
applies only to streams already in the tree.

Removing Streams
----------------

A stream can be entirely removed from the tree by calling
:meth:`remove_stream <priority.PriorityTree.remove_stream>`. Note that this is
*not* idempotent. Further, calling
:meth:`remove_stream <priority.PriorityTree.remove_stream>` and then re-adding
it *may* cause a substantial change in the shape of the priority tree, and
*will* cause the iteration order to change.
