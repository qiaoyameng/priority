# -*- coding: utf-8 -*-
"""
priority/tree
~~~~~~~~~~~~~

Implementation of the Priority tree data structure.
"""

import heapq

from typing import Any, Dict, List, Optional, Tuple


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


class InvalidSnapshotError(PriorityError):
    """
    A priority tree snapshot is malformed and cannot be restored.
    """

    pass


class UnsupportedSnapshotVersionError(InvalidSnapshotError):
    """
    A priority tree snapshot uses an unrecognised format version.
    """

    pass


SNAPSHOT_VERSION = 1
_SNAPSHOT_KEYS = {"version", "maximum_streams", "streams"}
_STREAM_SNAPSHOT_KEYS = {
    "id",
    "parent",
    "weight",
    "active",
    "children",
    "queue",
    "last_weight",
    "deficit",
}


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


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_snapshot(
    snapshot: Any,
    receiver_maximum_streams: Optional[int] = None,
) -> Tuple[int, Dict[int, Dict[str, Any]]]:
    if not isinstance(snapshot, dict):
        raise InvalidSnapshotError("Snapshot must be an object.")

    if "version" not in snapshot:
        raise InvalidSnapshotError("Snapshot is missing a format version.")
    if (
        not _is_int(snapshot["version"])
        or snapshot["version"] != SNAPSHOT_VERSION
    ):
        raise UnsupportedSnapshotVersionError(
            "Unsupported snapshot format version: %r" % (snapshot["version"],)
        )
    if set(snapshot) != _SNAPSHOT_KEYS:
        raise InvalidSnapshotError("Snapshot contains invalid top-level fields.")

    maximum_streams = snapshot["maximum_streams"]
    if not _is_int(maximum_streams) or maximum_streams <= 0:
        raise InvalidSnapshotError("maximum_streams must be a positive integer.")
    if (
        receiver_maximum_streams is not None
        and maximum_streams > receiver_maximum_streams
    ):
        raise TooManyStreamsError(
            "Snapshot allows more streams than the receiving priority tree."
        )

    raw_streams = snapshot["streams"]
    if not isinstance(raw_streams, list) or not raw_streams:
        raise InvalidSnapshotError("Snapshot must contain a non-empty stream list.")

    streams: Dict[int, Dict[str, Any]] = {}

    for raw_stream in raw_streams:
        if not isinstance(raw_stream, dict):
            raise InvalidSnapshotError("Each stream must be an object.")
        if set(raw_stream) != _STREAM_SNAPSHOT_KEYS:
            raise InvalidSnapshotError("Stream contains invalid fields.")

        stream_id = raw_stream["id"]
        if not _is_int(stream_id):
            raise InvalidSnapshotError("Stream IDs must be integers.")
        if stream_id in streams:
            raise DuplicateStreamError(
                "Stream %d appears more than once." % stream_id
            )

        parent_id = raw_stream["parent"]
        if stream_id != 0 and not _is_int(parent_id):
            raise InvalidSnapshotError(
                "Stream %d must have an integer parent." % stream_id
            )

        weight = raw_stream["weight"]
        if not _is_int(weight) or not (1 <= weight <= 256):
            raise BadWeightError(
                "Stream %d weight must be between 1 and 256 (inclusive)." % stream_id
            )

        active = raw_stream["active"]
        if not isinstance(active, bool):
            raise InvalidSnapshotError(
                "Stream %d active state must be a boolean." % stream_id
            )

        children = raw_stream["children"]
        if not isinstance(children, list) or not all(
            _is_int(child) for child in children
        ):
            raise InvalidSnapshotError(
                "Stream %d children must be a list of integers." % stream_id
            )
        if len(set(children)) != len(children):
            raise InvalidSnapshotError(
                "Stream %d has a duplicated child." % stream_id
            )

        queue = raw_stream["queue"]
        if not isinstance(queue, list):
            raise InvalidSnapshotError(
                "Stream %d queue must be a list of queue entries." % stream_id
            )

        queue_entries = []
        for entry in queue:
            if (
                not isinstance(entry, list)
                or len(entry) != 2
                or not _is_int(entry[0])
                or not _is_int(entry[1])
            ):
                raise InvalidSnapshotError(
                    "Stream %d has an invalid queue entry." % stream_id
                )
            level, child_id = entry
            if level < 0:
                raise InvalidSnapshotError(
                    "Stream %d has a negative queue level." % stream_id
                )
            queue_entries.append((level, child_id))

        last_weight = raw_stream["last_weight"]
        if not _is_int(last_weight) or last_weight < 0:
            raise InvalidSnapshotError(
                "Stream %d last_weight must be a non-negative integer." % stream_id
            )

        deficit = raw_stream["deficit"]
        if not _is_int(deficit) or not (0 <= deficit <= 255):
            raise InvalidSnapshotError(
                "Stream %d deficit must be between 0 and 255." % stream_id
            )

        streams[stream_id] = {
            "id": stream_id,
            "parent": parent_id,
            "weight": weight,
            "active": active,
            "children": children,
            "queue": queue_entries,
            "last_weight": last_weight,
            "deficit": deficit,
        }

    if 0 not in streams:
        raise InvalidSnapshotError("Snapshot must contain root stream 0.")

    root = streams[0]
    if (
        root["parent"] is not None
        or root["weight"] != 1
        or root["active"] is not False
        or root["deficit"] != 0
    ):
        raise InvalidSnapshotError("Root stream 0 has invalid state.")

    if len(streams) > maximum_streams:
        raise TooManyStreamsError(
            "Snapshot contains more streams than its maximum allows."
        )

    for stream_id, stream in streams.items():
        parent_id = stream["parent"]
        if stream_id != 0:
            if parent_id not in streams:
                raise InvalidSnapshotError(
                    "Stream %d has missing parent %d." % (stream_id, parent_id)
                )
            if stream_id not in streams[parent_id]["children"]:
                raise InvalidSnapshotError(
                    "Parent %d does not list stream %d as a child."
                    % (parent_id, stream_id)
                )

        for child_id in stream["children"]:
            if child_id not in streams:
                raise InvalidSnapshotError(
                    "Stream %d has missing child %d." % (stream_id, child_id)
                )
            if child_id == 0:
                raise InvalidSnapshotError("Root stream 0 cannot be a child.")
            if streams[child_id]["parent"] != stream_id:
                raise InvalidSnapshotError(
                    "Stream %d does not have parent %d." % (child_id, stream_id)
                )

    parent_state = {0: "done"}
    for start_stream_id in streams:
        if start_stream_id == 0:
            continue

        stream_id = start_stream_id
        path = []
        while stream_id not in parent_state:
            parent_state[stream_id] = "visiting"
            path.append(stream_id)
            stream_id = streams[stream_id]["parent"]

        if parent_state[stream_id] == "visiting":
            raise PriorityLoop("Snapshot contains a priority loop.")

        for visited_stream_id in path:
            parent_state[visited_stream_id] = "done"

    for stream_id, stream in streams.items():
        queue_child_ids = [child_id for level, child_id in stream["queue"]]
        if set(queue_child_ids) != set(stream["children"]):
            raise InvalidSnapshotError(
                "Stream %d queue does not match its children." % stream_id
            )
        if len(set(queue_child_ids)) != len(queue_child_ids):
            raise InvalidSnapshotError(
                "Stream %d queue contains a duplicated child." % stream_id
            )

        expected_heap = list(stream["queue"])
        heapq.heapify(expected_heap)
        if stream["queue"] != expected_heap:
            raise InvalidSnapshotError(
                "Stream %d queue is not in heap order." % stream_id
            )

    return maximum_streams, streams


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

    def to_dict(self) -> Dict[str, Any]:
        """
        Export a versioned, pickle-independent snapshot of the tree.

        The snapshot contains only JSON-compatible primitive types, lists, and
        dictionaries. Exporting does not advance the scheduler.
        """
        streams = []
        for stream_id in sorted(self._streams):
            stream = self._streams[stream_id]
            streams.append(
                {
                    "id": stream.stream_id,
                    "parent": (
                        None if stream.parent is None else stream.parent.stream_id
                    ),
                    "weight": stream.weight,
                    "active": stream.active,
                    "children": [child.stream_id for child in stream.children],
                    "queue": [
                        [level, child.stream_id]
                        for level, child in stream.child_queue
                    ],
                    "last_weight": stream.last_weight,
                    "deficit": stream._deficit,
                }
            )

        return {
            "version": SNAPSHOT_VERSION,
            "maximum_streams": self._maximum_streams,
            "streams": streams,
        }

    @classmethod
    def from_dict(
        cls,
        snapshot: Any,
        maximum_streams: Optional[int] = None,
    ) -> "PriorityTree":
        """
        Validate a snapshot and return a new tree containing its full state.
        """
        snapshot_maximum_streams, streams = _validate_snapshot(
            snapshot,
            maximum_streams,
        )

        tree = cls(maximum_streams=snapshot_maximum_streams)
        restored = {}

        for stream_id, stream_state in streams.items():
            stream = Stream(stream_id, stream_state["weight"])
            stream.active = stream_state["active"]
            stream.last_weight = stream_state["last_weight"]
            stream._deficit = stream_state["deficit"]
            restored[stream_id] = stream

        for stream_id, stream_state in streams.items():
            stream = restored[stream_id]
            stream.parent = (
                None
                if stream_state["parent"] is None
                else restored[stream_state["parent"]]
            )
            stream.children = [
                restored[child_id] for child_id in stream_state["children"]
            ]
            stream.child_queue = [
                (level, restored[child_id])
                for level, child_id in stream_state["queue"]
            ]

        tree._root_stream = restored[0]
        tree._streams = restored
        tree._maximum_streams = snapshot_maximum_streams
        return tree

    def export_state(self) -> Dict[str, Any]:
        """
        Export a versioned, pickle-independent snapshot of the tree.
        """
        return self.to_dict()

    @classmethod
    def restore_state(
        cls,
        snapshot: Any,
        maximum_streams: Optional[int] = None,
    ) -> "PriorityTree":
        """
        Validate a snapshot and return a new tree containing its full state.
        """
        return cls.from_dict(snapshot, maximum_streams=maximum_streams)

    def import_state(self, snapshot: Any) -> None:
        """
        Replace this tree's state with a fully validated snapshot.

        Validation completes before this tree is mutated, so an invalid
        snapshot leaves the receiving tree unchanged.
        """
        restored = self.restore_state(snapshot, self._maximum_streams)
        self._root_stream = restored._root_stream
        self._streams = restored._streams
        self._maximum_streams = restored._maximum_streams

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
                strip_children=False,
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
