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


class BadCostError(PriorityError):
    """
    An attempt was made to settle a grant with an invalid cost.
    """

    pass


class MissingGrantError(PriorityError):
    """
    An attempt was made to settle a grant that was never issued, or that has
    already been settled.
    """

    pass


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
        self._grant_outstanding = False
        self._grant_parent: Optional[Stream] = None
        self._pending_level = 0

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
        if child._grant_outstanding:
            # A stream with an outstanding grant must not be present in any
            # scheduling queue: it is re-inserted when the grant is settled.
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

    def schedule_grant(self) -> int:
        """
        Returns the stream ID of the next child to schedule, and takes out a
        grant on it. The selected stream is held out of every scheduling
        queue until the grant is settled with the real cost of the data that
        was sent. Potentially recurses down the tree of priorities.
        """
        # Cannot be called on active streams.
        assert not self.active

        next_stream = None
        popped_streams = []

        # Spin looking for the next active stream. Everything we pop off has
        # to be rescheduled, even if it turns out none of them were active at
        # this time. The stream the grant is issued for is the exception: it
        # stays out of the queue until its grant is settled.
        try:
            while next_stream is None:
                # If the queue is empty, immediately fail.
                val = heapq.heappop(self.child_queue)
                level, child = val

                if child.active:
                    next_stream = child.stream_id
                    child._grant_outstanding = True
                    child._grant_parent = self
                    child._pending_level = level
                    self.last_weight = level
                else:
                    popped_streams.append(val)
                    # Guard against the possibility that the child also has no
                    # suitable children.
                    try:
                        next_stream = child.schedule_grant()
                    except IndexError:
                        continue
        finally:
            for level, child in popped_streams:
                self.last_weight = level
                level += (256 + child._deficit) // child.weight
                child._deficit = (256 + child._deficit) % child.weight
                heapq.heappush(self.child_queue, (level, child))

        return next_stream

    def _settle_grant(self, cost: int, in_tree: bool) -> None:
        """
        Settles an outstanding grant with the real cost of the data sent,
        re-inserting the stream into its parent's scheduling queue.
        """
        grant_parent = self._grant_parent
        pending_level = self._pending_level
        self._grant_outstanding = False
        self._grant_parent = None
        self._pending_level = 0

        if not in_tree:
            # The stream was removed from the tree while the grant was
            # outstanding. The cost is discarded along with the stream so
            # that no debt is left behind for the remaining streams.
            self._deficit = 0
            return

        increment, self._deficit = divmod(cost + self._deficit, self.weight)

        parent = self.parent
        if parent is grant_parent:
            level = pending_level + increment
        else:
            # The stream was re-attached elsewhere in the tree while waiting
            # for settlement: charge the cost against the new parent's
            # current scheduling level.
            level = parent.last_weight + increment  # type: ignore[union-attr]

        heapq.heappush(parent.child_queue, (level, self))  # type: ignore[union-attr]

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
        self._outstanding_grants: Dict[int, Stream] = {}

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
            self._streams[stream_id].active = False
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

    def __next__(self) -> int:  # pragma: no cover
        try:
            return self._root_stream.schedule()
        except IndexError:
            raise DeadlockError("No unblocked streams to schedule.")

    def next(self) -> int:  # pragma: no cover
        return self.__next__()

    def next_grant(self) -> int:
        """
        Selects the next stream to schedule and takes out a grant on it.

        Unlike :meth:`next <PriorityTree.next>`, which implicitly charges
        each selected stream a fixed quantum, this method defers the
        accounting: the caller reports the real number of bytes sent by
        calling :meth:`settle <PriorityTree.settle>` (or returns the grant
        unused via :meth:`return_grant <PriorityTree.return_grant>`). This
        makes the bandwidth share of each stream proportional to its weight
        even when streams send frames of very different sizes.

        A stream with an outstanding grant is held out of the scheduling
        queues, so it cannot be selected again (by either this method or
        :meth:`next <PriorityTree.next>`) until the grant is settled. If the
        stream is blocked, removed, or re-attached elsewhere in the tree
        while its grant is outstanding, the grant remains valid and must
        still be settled: the cost is then charged against the stream's new
        position, or discarded entirely if the stream is no longer in the
        tree.

        This interface and :meth:`next <PriorityTree.next>` may be freely
        mixed on the same tree: both maintain the same scheduling invariants.
        Note, however, that :meth:`next <PriorityTree.next>` charges a fixed
        quantum per selection, so byte-level fairness is only guaranteed when
        all selections go through the grant-based interface.

        :returns: The stream ID of the stream that should next send data.
        :raises DeadlockError: If there are no unblocked streams to schedule.
        """
        try:
            stream_id = self._root_stream.schedule_grant()
        except IndexError:
            raise DeadlockError("No unblocked streams to schedule.")

        self._outstanding_grants[stream_id] = self._streams[stream_id]
        return stream_id

    def settle(self, stream_id: int, cost: int) -> None:
        """
        Settles the outstanding grant for a stream, reporting the real cost
        of the data that was sent for it.

        The cost is charged against the stream's scheduling position,
        divided by its weight, so that streams accumulate scheduling debt in
        proportion to ``cost / weight``. A cost of zero is allowed and means
        nothing was sent: the grant is returned unused and the stream
        resumes its former position. Negative or non-integer costs are
        rejected with :class:`BadCostError <priority.BadCostError>`, and the
        grant remains outstanding so it can be settled correctly.

        The stream does not need to still be in the tree: if it was removed
        while the grant was outstanding, the cost is discarded and no
        residual debt is left behind for the other streams.

        :param stream_id: The ID of the stream whose grant is being settled.
        :param cost: The number of bytes actually sent for this grant. Must
            be a non-negative integer.
        :raises BadCostError: If the cost is negative or not an integer.
        :raises MissingGrantError: If the stream has no outstanding grant.
        :raises MissingStreamError: If the stream is unknown to the tree and
            has no outstanding grant.
        """
        if isinstance(cost, bool) or not isinstance(cost, int):
            raise BadCostError("Cost must be an integer")
        if cost < 0:
            raise BadCostError("Cost must not be negative")

        try:
            stream = self._outstanding_grants.pop(stream_id)
        except KeyError:
            if stream_id in self._streams:
                raise MissingGrantError(
                    "Stream %d has no outstanding grant" % stream_id
                )
            raise MissingStreamError("Stream %d not in tree" % stream_id)

        in_tree = self._streams.get(stream_id) is stream
        stream._settle_grant(cost, in_tree)

    def return_grant(self, stream_id: int) -> None:
        """
        Returns an outstanding grant unused, without charging any cost.

        This is exactly equivalent to calling :meth:`settle
        <PriorityTree.settle>` with a cost of zero.

        :param stream_id: The ID of the stream whose grant is being returned.
        """
        self.settle(stream_id, 0)
