# -*- coding: utf-8 -*-
"""
priority/tree
~~~~~~~~~~~~~

Implementation of the Priority tree data structure.
"""

import copy
import heapq
from dataclasses import dataclass

from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union


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
        heapq.heappush(self.child_queue, (self.last_weight, child))

    def add_child_exclusive(self, child: "Stream") -> None:
        """
        Add a stream that exclusively depends on this one.

        :param child: A ``Stream`` object that exclusively depends on this one.
        """
        old_children = [
            old_child for old_child in self.children if old_child != child
        ]
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


@dataclass(frozen=True)
class PriorityUpdate:
    """
    A single priority change in a batch update.

    :param stream_id: The ID of the stream being updated.
    :param depends_on: The ID of the stream it now depends on. ``None`` or
        ``0`` means the root stream.
    :param weight: The new weight for the stream.
    :param exclusive: Whether the dependency is exclusive.
    """

    stream_id: int
    depends_on: Optional[int] = None
    weight: int = 16
    exclusive: bool = False


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

    @staticmethod
    def _parse_priority_update(
        update: Union[PriorityUpdate, Mapping[str, Any], Tuple[Any, ...]]
    ) -> PriorityUpdate:
        """
        Return a batch update as a PriorityUpdate.
        """
        if isinstance(update, PriorityUpdate):
            return update

        if isinstance(update, Mapping):
            try:
                stream_id = update["stream_id"]
            except KeyError:
                raise TypeError("Priority updates must contain a stream_id")

            return PriorityUpdate(
                stream_id=stream_id,
                depends_on=update.get("depends_on"),
                weight=update.get("weight", 16),
                exclusive=update.get("exclusive", False),
            )

        if isinstance(update, tuple):
            if not 1 <= len(update) <= 4:
                raise TypeError("Priority update tuples may contain 1 to 4 fields")

            stream_id = update[0]
            depends_on = update[1] if len(update) >= 2 else None
            weight = update[2] if len(update) >= 3 else 16
            exclusive = update[3] if len(update) >= 4 else False
            return PriorityUpdate(
                stream_id=stream_id,
                depends_on=depends_on,
                weight=weight,
                exclusive=exclusive,
            )

        raise TypeError(
            "Priority updates must be PriorityUpdate objects, mappings, or tuples"
        )

    def _validate_priority_tree(self) -> None:
        """
        Ensure that all streams form one consistent tree rooted at stream 0.
        """
        root = self._root_stream

        if self._streams.get(0) is not root or root.parent is not None:
            raise PriorityLoop("Priority tree does not have a single root")

        seen = {0}
        pending = [root]

        while pending:
            stream = pending.pop()
            child_ids = {id(child) for child in stream.children}
            queued_ids = {id(child) for _, child in stream.child_queue}

            if child_ids != queued_ids or len(stream.child_queue) != len(
                stream.children
            ):
                raise PriorityLoop("Priority tree child queues are inconsistent")

            for child in stream.children:
                if child.parent is not stream:
                    raise PriorityLoop("Priority tree parent links are inconsistent")
                if child.stream_id in seen:
                    raise PriorityLoop(
                        "Stream %d is in a priority loop." % child.stream_id
                    )
                if self._streams.get(child.stream_id) is not child:
                    raise PriorityLoop("Priority tree contains an unknown stream")

                seen.add(child.stream_id)
                pending.append(child)

        if seen != set(self._streams):
            raise PriorityLoop("Priority tree contains an unreachable stream")

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

    def reprioritize_many(
        self,
        updates: Iterable[
            Union[PriorityUpdate, Mapping[str, Any], Tuple[Any, ...]]
        ],
        *,
        create_missing_parents: bool = True,
    ) -> None:
        """
        Apply a batch of priority updates atomically.

        Updates are interpreted in their given order. In particular, an
        exclusive update moves all children currently attached to its new
        parent, including children moved there by earlier updates in the same
        batch.

        :param updates: An iterable of :class:`PriorityUpdate` objects or
            mappings with ``stream_id``, ``depends_on``, ``weight`` and
            ``exclusive`` keys.
        :param create_missing_parents: Whether to insert blocked placeholder
            streams for parent IDs that are not already present. If ``False``,
            a missing parent raises :class:`MissingStreamError`.
        """
        adjustments = [self._parse_priority_update(update) for update in updates]

        for adjustment in adjustments:
            if adjustment.stream_id == 0:
                raise PseudoStreamError("Cannot reprioritize stream 0")
            if adjustment.stream_id not in self._streams:
                raise MissingStreamError(
                    "Stream %d not in tree" % adjustment.stream_id
                )

        missing_parent_ids = []

        for adjustment in adjustments:
            Stream(adjustment.stream_id, adjustment.weight)

            parent_id = adjustment.depends_on or 0
            if parent_id == adjustment.stream_id:
                raise PriorityLoop(
                    "Stream %d must not depend on itself" % adjustment.stream_id
                )

            if parent_id not in self._streams:
                if not create_missing_parents:
                    raise MissingStreamError(
                        "Stream %d not in tree" % parent_id
                    )
                if parent_id not in missing_parent_ids:
                    missing_parent_ids.append(parent_id)

        if len(self._streams) + len(missing_parent_ids) > self._maximum_streams:
            raise TooManyStreamsError(
                "Refusing to insert more than %d streams into priority tree"
                % self._maximum_streams
            )

        candidate = copy.deepcopy(self)

        for parent_id in missing_parent_ids:
            candidate.insert_stream(parent_id)
            candidate.block(parent_id)

        for adjustment in adjustments:
            candidate.reprioritize(
                stream_id=adjustment.stream_id,
                depends_on=adjustment.depends_on,
                weight=adjustment.weight,
                exclusive=adjustment.exclusive,
            )

        candidate._validate_priority_tree()

        self._root_stream = candidate._root_stream
        self._streams = candidate._streams

    batch_reprioritize = reprioritize_many
    bulk_reprioritize = reprioritize_many
    reprioritize_batch = reprioritize_many

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
