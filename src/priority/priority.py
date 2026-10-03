# -*- coding: utf-8 -*-
"""
priority/tree
~~~~~~~~~~~~~

Implementation of the Priority tree data structure.
"""

import heapq

from typing import Dict, List, Tuple, Optional


class PriorityError(Exception):
    """
    The base class for all ``priority`` exceptions.
    """


class DeadlockError(PriorityError):
    """
    Raised when there are no streams that can make progress: all streams are
    blocked.
    """

    pass


class PriorityLoop(PriorityError):
    """
    An unexpected priority loop has been detected. The tree is invalid.
    """

    pass


class DuplicateStreamError(PriorityError):
    """
    An attempt was made to insert a stream that already exists.
    """

    pass


class MissingStreamError(KeyError, PriorityError):
    """
    An operation was attempted on a stream that is not present in the tree.
    """

    pass


class TooManyStreamsError(PriorityError):
    """
    An attempt was made to insert a dangerous number of streams into the
    priority tree at the same time.

    .. versionadded:: 1.2.0
    """

    pass


class BadWeightError(PriorityError):
    """
    An attempt was made to create a stream with an invalid weight.

    .. versionadded:: 1.3.0
    """

    pass


class PseudoStreamError(PriorityError):
    """
    An operation was attempted on stream 0.

    .. versionadded:: 1.3.0
    """

    pass


class InvalidGrantError(PriorityError):
    """
    An operation was attempted with a grant that is not outstanding.
    """

    pass


class InvalidCostError(PriorityError):
    """
    A settlement was attempted with a cost that is not a non-negative integer.
    """

    pass


class Grant:
    """
    A one-time opportunity to send data on one stream.

    Grants are returned by :meth:`PriorityTree.acquire_grant` and must be
    settled with :meth:`PriorityTree.settle_grant` or returned with
    :meth:`PriorityTree.return_grant`.

    :param stream_id: The ID of the stream that the grant applies to.
    """

    __slots__ = ("stream_id",)

    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id

    def __repr__(self) -> str:
        return "Grant<stream_id=%d>" % self.stream_id


class Stream:
    """
    Priority information for a given stream.

    :param stream_id: The stream ID for the new stream.
    :param weight: (optional) The stream weight. Defaults to 16.
    """

    def __init__(self, stream_id: int, weight: int = 16) -> None:
        self.stream_id = stream_id
        self.weight = weight
        self.children: List[Stream] = []
        self.parent: Optional[Stream] = None
        self.child_queue: List[Tuple[int, Stream]] = []
        self.active = True
        self.last_weight = 0
        self._deficit = 0
        self._outstanding = False
        self._held_level = 0
        self._held_cost = 0

    @property
    def weight(self) -> int:
        return self._weight

    @weight.setter
    def weight(self, value: int) -> None:
        # RFC 7540 § 5.3.2: "All dependent streams are allocated an integer
        # weight between 1 and 256 (inclusive)."
        if not isinstance(value, int):
            raise BadWeightError("Stream weight should be an integer")
        elif not (1 <= value <= 256):
            raise BadWeightError("Stream weight must be between 1 and 256 (inclusive)")
        self._weight = value

    def add_child(self, child: "Stream") -> None:
        """
        Add a stream that depends on this one.

        :param child: A ``Stream`` object that depends on this one.
        """
        child.parent = self
        self.children.append(child)

        if child._outstanding and child.active:
            child._held_level = self.last_weight
            return

        heapq.heappush(self.child_queue, (self.last_weight, child))

    def add_child_exclusive(self, child: "Stream") -> None:
        """
        Add a stream that exclusively depends on this one.

        :param child: A ``Stream`` object that exclusively depends on this one.
        """
        old_children = self.children
        self.children = []
        self.child_queue = []
        self.last_weight = 0
        self.add_child(child)

        for old_child in old_children:
            child.add_child(old_child)

    def remove_child(
        self,
        child: "Stream",
        strip_children: bool = True,
    ) -> None:
        """
        Removes a child stream from this stream. This is a potentially somewhat
        expensive operation.

        :param child: The child stream to remove.
        :param strip_children: Whether children of the removed stream should
            become children of this stream.
        """
        # To do this we do the following:
        #
        # - remove the child stream from the list of children
        # - build a new priority queue, filtering out the child when we find
        #   it in the old one
        self.children.remove(child)

        new_queue: List[Tuple[int, Stream]] = []

        while self.child_queue:
            level, stream = heapq.heappop(self.child_queue)
            if stream == child:
                continue

            heapq.heappush(new_queue, (level, stream))

        self.child_queue = new_queue

        if strip_children:
            for new_child in child.children:
                self.add_child(new_child)

    def schedule(self) -> int:
        """
        Returns the stream ID of the next child to schedule. Potentially
        recurses down the tree of priorities.
        """
        # Cannot be called on active streams.
        assert not self.active

        next_stream = None
        popped_streams = []

        # Spin looking for the next active stream. Everything we pop off has
        # to be rescheduled, even if it turns out none of them were active at
        # this time.
        try:
            while next_stream is None:
                # If the queue is empty, immediately fail.
                val = heapq.heappop(self.child_queue)
                popped_streams.append(val)
                level, child = val

                if child.active:
                    next_stream = child.stream_id
                else:
                    # Guard against the possibility that the child also has no
                    # suitable children.
                    try:
                        next_stream = child.schedule()
                    except IndexError:
                        continue
        finally:
            for level, child in popped_streams:
                self.last_weight = level
                level += (256 + child._deficit) // child.weight
                child._deficit = (256 + child._deficit) % child.weight
                heapq.heappush(self.child_queue, (level, child))

        return next_stream

    def acquire(self) -> "Stream":
        """
        Returns the next active child that does not have an outstanding grant.

        The selected stream is removed from its parent's queue until its grant
        is settled. All other queue entries are left unchanged.
        """
        assert not self.active

        next_stream: Optional[Stream] = None
        popped_entries: List[Tuple[int, Stream]] = []

        try:
            while self.child_queue:
                level, child = heapq.heappop(self.child_queue)
                popped_entries.append((level, child))

                if child.active:
                    if child._outstanding:
                        continue

                    child._outstanding = True
                    child._held_level = level
                    child._held_cost = 0
                    self.last_weight = level
                    next_stream = child
                    break

                try:
                    next_stream = child.acquire()
                except IndexError:
                    continue
                self.last_weight = level
                break
        finally:
            for level, child in popped_entries:
                if child is next_stream:
                    continue
                heapq.heappush(self.child_queue, (level, child))

        if next_stream is None:
            raise IndexError("No unblocked streams to schedule.")

        return next_stream

    # Custom repr
    def __repr__(self) -> str:
        return "Stream<id=%d, weight=%d>" % (self.stream_id, self.weight)

    # Custom comparison
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Stream):  # pragma: no cover
            return False

        return self.stream_id == other.stream_id

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __lt__(self, other: "Stream") -> bool:
        if not isinstance(other, Stream):  # pragma: no cover
            return NotImplemented

        return self.stream_id < other.stream_id

    def __le__(self, other: "Stream") -> bool:
        if not isinstance(other, Stream):  # pragma: no cover
            return NotImplemented

        return self.stream_id <= other.stream_id

    def __gt__(self, other: "Stream") -> bool:
        if not isinstance(other, Stream):  # pragma: no cover
            return NotImplemented

        return self.stream_id > other.stream_id

    def __ge__(self, other: "Stream") -> bool:
        if not isinstance(other, Stream):  # pragma: no cover
            return NotImplemented

        return self.stream_id >= other.stream_id


def _stream_cycle(new_parent: Stream, current: Stream) -> bool:
    """
    Reports whether the new parent depends on the current stream.
    """
    parent = new_parent

    # Don't iterate forever, but instead assume that the tree doesn't
    # get more than 100 streams deep. This should catch accidental
    # tree loops. This is the definition of defensive programming.
    for _ in range(100):
        parent = parent.parent  # type: ignore[assignment]
        if parent.stream_id == current.stream_id:
            return True
        elif parent.stream_id == 0:
            return False

    raise PriorityLoop(
        "Stream %d is in a priority loop." % new_parent.stream_id
    )  # pragma: no cover


class PriorityTree:
    """
    A HTTP/2 Priority Tree.

    This tree stores HTTP/2 streams according to their HTTP/2 priorities.

    .. versionchanged:: 1.2.0
       Added ``maximum_streams`` keyword argument.

    :param maximum_streams: The maximum number of streams that may be active in
        the priority tree at any one time. If this number is exceeded, the
        priority tree will raise a :class:`TooManyStreamsError
        <priority.TooManyStreamsError>` and will refuse to insert the stream.

        This parameter exists to defend against the possibility of DoS attack
        by attempting to overfill the priority tree. If any endpoint is
        attempting to manage the priority of this many streams at once it is
        probably trying to screw with you, so it is sensible to simply refuse
        to play ball at that point.

        While we allow the user to configure this, we don't really *expect*
        them too, unless they want to be even more conservative than we are by
        default.
    :type maximum_streams: ``int``
    """

    def __init__(self, maximum_streams: int = 1000) -> None:
        # This flat array keeps hold of all the streams that are logically
        # dependent on stream 0.
        self._root_stream = Stream(stream_id=0, weight=1)
        self._root_stream.active = False
        self._streams = {0: self._root_stream}
        self._grants: Dict[Grant, Stream] = {}
        self._legacy_scheduling_used = False
        self._cost_scheduling_used = False

        if not isinstance(maximum_streams, int):
            raise TypeError("maximum_streams must be an int.")
        if maximum_streams <= 0:
            raise ValueError("maximum_streams must be a positive integer.")
        self._maximum_streams = maximum_streams

    def _get_or_insert_parent(self, parent_stream_id: int) -> Stream:
        """
        When inserting or reprioritizing a stream it is possible to make it
        dependent on a stream that is no longer in the tree. In this situation,
        rather than bail out, we should insert the parent stream into the tree
        with default priority and mark it as blocked.
        """
        try:
            return self._streams[parent_stream_id]
        except KeyError:
            self.insert_stream(parent_stream_id)
            self.block(parent_stream_id)
            return self._streams[parent_stream_id]

    def _exclusive_insert(
        self,
        parent_stream: Stream,
        inserted_stream: Stream,
    ) -> None:
        """
        Insert ``inserted_stream`` beneath ``parent_stream``, obeying the
        semantics of exclusive insertion.
        """
        parent_stream.add_child_exclusive(inserted_stream)

    def insert_stream(
        self,
        stream_id: int,
        depends_on: Optional[int] = None,
        weight: int = 16,
        exclusive: bool = False,
    ) -> None:
        """
        Insert a stream into the tree.

        :param stream_id: The stream ID of the stream being inserted.
        :param depends_on: (optional) The ID of the stream that the new stream
            depends on, if any.
        :param weight: (optional) The weight to give the new stream. Defaults
            to 16.
        :param exclusive: (optional) Whether this new stream should be an
            exclusive dependency of the parent.
        """
        if stream_id in self._streams:
            raise DuplicateStreamError("Stream %d already in tree" % stream_id)

        if (len(self._streams) + 1) > self._maximum_streams:
            raise TooManyStreamsError(
                "Refusing to insert %d streams into priority tree at once"
                % (self._maximum_streams + 1)
            )

        stream = Stream(stream_id, weight)

        if not depends_on:
            depends_on = 0
        elif depends_on == stream_id:
            raise PriorityLoop("Stream %d must not depend on itself." % stream_id)

        if exclusive:
            parent_stream = self._get_or_insert_parent(depends_on)
            self._exclusive_insert(parent_stream, stream)
            self._streams[stream_id] = stream
            return

        parent = self._get_or_insert_parent(depends_on)
        parent.add_child(stream)
        self._streams[stream_id] = stream

    def reprioritize(
        self,
        stream_id: int,
        depends_on: Optional[int] = None,
        weight: int = 16,
        exclusive: bool = False,
    ) -> None:
        """
        Update the priority status of a stream already in the tree.

        :param stream_id: The stream ID of the stream being updated.
        :param depends_on: (optional) The ID of the stream that the stream now
            depends on. If ``None``, will be moved to depend on stream 0.
        :param weight: (optional) The new weight to give the stream. Defaults
            to 16.
        :param exclusive: (optional) Whether this stream should now be an
            exclusive dependency of the new parent.
        """
        if stream_id == 0:
            raise PseudoStreamError("Cannot reprioritize stream 0")

        try:
            current_stream = self._streams[stream_id]
        except KeyError:
            raise MissingStreamError("Stream %d not in tree" % stream_id)

        # Update things in a specific order to make sure the calculation
        # behaves properly. Specifically, we first update the weight. Then,
        # we check whether this stream is being made dependent on one of its
        # own dependents. Then, we remove this stream from its current parent
        # and move it to its new parent, taking its children with it.
        if depends_on:
            if depends_on == stream_id:
                raise PriorityLoop("Stream %d must not depend on itself" % stream_id)

            new_parent = self._get_or_insert_parent(depends_on)
            cycle = _stream_cycle(new_parent, current_stream)
        else:
            new_parent = self._streams[0]
            cycle = False

        current_stream.weight = weight

        # Our new parent is currently dependent on us. We should remove it from
        # its parent, and make it a child of our current parent, and then
        # continue.
        if cycle:
            new_parent.parent.remove_child(  # type: ignore[union-attr]
                new_parent,
            )
            current_stream.parent.add_child(  # type: ignore[union-attr]
                new_parent,
            )

        current_stream.parent.remove_child(  # type: ignore[union-attr]
            current_stream, strip_children=False
        )

        if exclusive:
            new_parent.add_child_exclusive(current_stream)
        else:
            new_parent.add_child(current_stream)

    def remove_stream(self, stream_id: int) -> None:
        """
        Removes a stream from the priority tree.

        :param stream_id: The ID of the stream to remove.
        """
        if stream_id == 0:
            raise PseudoStreamError("Cannot remove stream 0")

        try:
            child = self._streams.pop(stream_id)
        except KeyError:
            raise MissingStreamError("Stream %d not in tree" % stream_id)

        parent = child.parent
        parent.remove_child(child)  # type: ignore[union-attr]

    def block(self, stream_id: int) -> None:
        """
        Marks a given stream as blocked, with no data to send.

        :param stream_id: The ID of the stream to block.
        """
        if stream_id == 0:
            raise PseudoStreamError("Cannot block stream 0")

        try:
            stream = self._streams[stream_id]
            stream.active = False
            if stream._outstanding:
                parent = stream.parent
                if parent is not None and not self._has_queue_entry(parent, stream):
                    heapq.heappush(
                        parent.child_queue,
                        (stream._held_level + stream._held_cost, stream),
                    )
        except KeyError:
            raise MissingStreamError("Stream %d not in tree" % stream_id)

    def unblock(self, stream_id: int) -> None:
        """
        Marks a given stream as unblocked, with more data to send.

        :param stream_id: The ID of the stream to unblock.
        """
        if stream_id == 0:
            raise PseudoStreamError("Cannot unblock stream 0")

        try:
            self._streams[stream_id].active = True
        except KeyError:
            raise MissingStreamError("Stream %d not in tree" % stream_id)

    # The iterator protocol
    def __iter__(self) -> "PriorityTree":  # pragma: no cover
        return self

    def _next_stream(self) -> int:
        if self._cost_scheduling_used:
            raise PriorityError(
                "Cannot use next() on a tree that uses cost-based scheduling"
            )
        self._legacy_scheduling_used = True
        try:
            return self._root_stream.schedule()
        except IndexError:
            raise DeadlockError("No unblocked streams to schedule.")

    def __next__(self) -> int:  # pragma: no cover
        return self._next_stream()

    def next(self) -> int:  # pragma: no cover
        return self._next_stream()

    def acquire_grant(self) -> Grant:
        """
        Acquire a one-time opportunity to send data on the next stream.

        Unlike iterating over the tree, no cost is charged when the grant is
        acquired. Call :meth:`settle_grant` with the number of bytes actually
        sent, or :meth:`return_grant` if the opportunity was not used.

        A stream with an outstanding grant will not be selected again until
        that grant is settled or returned. Grants to different streams may be
        outstanding concurrently. Cost-based scheduling and the legacy
        iterator interface cannot be used on the same tree.
        """
        if self._legacy_scheduling_used:
            raise PriorityError(
                "Cannot use cost-based scheduling on a tree that uses next()"
            )
        self._cost_scheduling_used = True

        try:
            stream = self._root_stream.acquire()
        except IndexError:
            raise DeadlockError("No unblocked streams to schedule.")

        grant = Grant(stream.stream_id)
        self._grants[grant] = stream
        return grant

    def settle_grant(self, grant: Grant, bytes_sent: int) -> None:
        """
        Settle a grant using the number of bytes actually sent.

        Settling with zero bytes is equivalent to returning an unused grant.
        If the stream was removed after the grant was acquired, the grant is
        discarded and its cost is not charged to its former parent or children.

        :param grant: A grant returned by :meth:`acquire_grant`.
        :param bytes_sent: The non-negative integer number of bytes sent.
        """
        stream = self._validate_grant(grant)
        self._validate_cost(bytes_sent)

        self._grants.pop(grant)

        if self._streams.get(stream.stream_id) is not stream:
            stream._outstanding = False
            stream._held_level = 0
            stream._held_cost = 0
            return

        if bytes_sent == 0:
            self._finish_grant(stream)
            return

        self._charge_cost(stream, bytes_sent)
        self._finish_grant(stream)

    def return_grant(self, grant: Grant) -> None:
        """
        Return an unused grant without charging any cost.

        :param grant: A grant returned by :meth:`acquire_grant`.
        """
        stream = self._validate_grant(grant)
        self._grants.pop(grant)

        if self._streams.get(stream.stream_id) is not stream:
            stream._outstanding = False
            stream._held_level = 0
            stream._held_cost = 0
            return

        self._finish_grant(stream)

    def _validate_grant(self, grant: Grant) -> Stream:
        stream = self._grants.get(grant)
        if stream is None:
            raise InvalidGrantError("Grant is not outstanding")
        return stream

    @staticmethod
    def _validate_cost(bytes_sent: int) -> None:
        if isinstance(bytes_sent, bool) or not isinstance(bytes_sent, int):
            raise InvalidCostError("Sent bytes must be an integer")
        if bytes_sent < 0:
            raise InvalidCostError("Sent bytes must be non-negative")

    def _charge_cost(self, stream: Stream, bytes_sent: int) -> None:
        child = stream

        while child is not self._root_stream:
            parent = child.parent
            total = bytes_sent + child._deficit
            increment = total // child.weight
            child._deficit = total % child.weight

            level = self._remove_queue_entry(parent, child)
            if level is None:
                child._held_cost += increment
                parent.last_weight = child._held_level + child._held_cost
            else:
                parent.last_weight = level
                heapq.heappush(parent.child_queue, (level + increment, child))

            child = parent

    def _finish_grant(self, stream: Stream) -> None:
        stream._outstanding = False
        if stream.active:
            self._restore_held_stream(stream)
        stream._held_level = 0
        stream._held_cost = 0

    def _restore_held_stream(self, stream: Stream) -> None:
        parent = stream.parent
        if parent is None or parent is self._root_stream and stream.stream_id == 0:
            return

        if self._remove_queue_entry(parent, stream) is None:
            level = stream._held_level
            heapq.heappush(
                parent.child_queue,
                (level + stream._held_cost, stream),
            )
            stream._held_cost = 0

    @staticmethod
    def _remove_queue_entry(parent: Stream, child: Stream) -> Optional[int]:
        new_queue: List[Tuple[int, Stream]] = []
        removed_level = None

        while parent.child_queue:
            level, queued_child = heapq.heappop(parent.child_queue)
            if queued_child is child and removed_level is None:
                removed_level = level
            else:
                heapq.heappush(new_queue, (level, queued_child))

        parent.child_queue = new_queue
        return removed_level

    @staticmethod
    def _has_queue_entry(parent: Stream, child: Stream) -> bool:
        return any(queued_child is child for _, queued_child in parent.child_queue)
