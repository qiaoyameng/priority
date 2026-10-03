==========================================
Priority: A HTTP/2 Priority Implementation
==========================================

.. image:: https://github.com/python-hyper/priority/workflows/CI/badge.svg
    :target: https://github.com/python-hyper/priority/actions
    :alt: Build Status
.. image:: https://codecov.io/gh/python-hyper/priority/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/python-hyper/priority
    :alt: Code Coverage
.. image:: https://readthedocs.org/projects/priority/badge/?version=latest
    :target: https://priority.readthedocs.io/en/latest/
    :alt: Documentation Status
.. image:: https://img.shields.io/badge/chat-join_now-brightgreen.svg
    :target: https://gitter.im/python-hyper/community
    :alt: Chat community

.. image:: https://raw.github.com/python-hyper/documentation/master/source/logo/hyper-black-bg-white.png


Priority is a pure-Python implementation of the priority logic for HTTP/2, set
out in `RFC 7540 Section 5.3 (Stream Priority)`_. This logic allows for clients
to express a preference for how the server allocates its (limited) resources to
the many outstanding HTTP requests that may be running over a single HTTP/2
connection.

Specifically, this Python implementation uses a variant of the implementation
used in the excellent `H2O`_ project. This original implementation is also the
inspiration for `nghttp2's`_ priority implementation, and generally produces a
very clean and even priority stream. The only notable changes from H2O's
implementation are small modifications to allow the priority implementation to
work cleanly as a separate implementation, rather than being embedded in a
HTTP/2 stack directly.

While priority information in HTTP/2 is only a suggestion, rather than an
enforceable constraint, where possible servers should respect the priority
requests of their clients.

Using Priority
--------------

Priority has a simple API. Streams are inserted into the tree: when they are
inserted, they may optionally have a weight, depend on another stream, or
become an exclusive dependent of another stream.

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
~~~~~~~~~~~~~~~~~~

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

Settling Actual Costs
~~~~~~~~~~~~~~~~~~~~~

Iteration allocates opportunities, not bytes. If one stream sends 1 KB each
time it is selected and another sends 64 KB, their cumulative bandwidth will
not be allocated according to their weights. For those cases, use the
explicit grant interface instead:

.. code-block:: python

    >>> grant = p.acquire_grant()
    >>> bytes_sent = send_data(grant.stream_id)
    >>> p.settle_grant(grant, bytes_sent)

Each call returns one grant; grants to different streams may be outstanding
at the same time. No scheduling cost is charged until the caller reports the
number of bytes actually sent. If the opportunity is not used, return the
grant or settle it with ``0``:

.. code-block:: python

    >>> p.return_grant(grant)
    >>> # This is equivalent:
    >>> p.settle_grant(grant, 0)

Only one outstanding grant can exist for a given stream. If the stream is
blocked, removed, or reprioritized while the grant is outstanding, the tree
still keeps that opportunity separate; settling or returning it afterwards
will not charge a removed stream's cost against another stream.

The iterator and grant interfaces must not be mixed on the same tree. Choose
``next()`` when every opportunity has the same cost, and use grants when
different sends have different byte counts. Costs must be non-negative
integers; negative or non-integer values are rejected.

Updating The Tree
~~~~~~~~~~~~~~~~~

If for any reason a stream is unable to proceed (for example, it is blocked on
HTTP/2 flow control, or it is waiting for more data from another service), that
stream is *blocked*. The ``PriorityTree`` should be informed that the stream is
blocked so that other dependent streams get a chance to proceed. This can be
done by calling the ``block`` method of the tree with the stream ID that is
currently unable to proceed. This will automatically update the tree, and it
will adjust on the fly to correctly allow any streams that were dependent on
the blocked one to progress.

For example:

.. code-block:: python

    >>> for stream_id in p:
    ...     send_data(stream_id)
    ...     if blocked(stream_id):
    ...         p.block(stream_id)

When a stream goes from being blocked to being unblocked, call the ``unblock``
method to place it back into the sequence. Both the ``block`` and ``unblock``
methods are idempotent and safe to call repeatedly.

Additionally, the priority of a stream may change. When it does, the
``reprioritize`` method can be used to update the tree in the wake of that
change. ``reprioritize`` has the same signature as ``insert_stream``, but
applies only to streams already in the tree.

Removing Streams
~~~~~~~~~~~~~~~~

A stream can be entirely removed from the tree by calling ``remove_stream``.
Note that this is not idempotent. Further, calling ``remove_stream`` and then
re-adding it *may* cause a substantial change in the shape of the priority
tree, and *will* cause the iteration order to change.

License
-------

Priority is made available under the MIT License. For more details, see the
LICENSE file in the repository.

Authors
-------

Priority is maintained by Cory Benfield, with contributions from others. For
more details about the contributors, please see CONTRIBUTORS.rst in the
repository.


.. _RFC 7540 Section 5.3 (Stream Priority): https://tools.ietf.org/html/rfc7540#section-5.3
.. _nghttp2's: https://nghttp2.org/blog/2015/11/11/stream-scheduling-utilizing-http2-priority/
.. _H2O: https://h2o.examp1e.net/
