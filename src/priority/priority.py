# -*- coding: utf-8 -*-
"""
priority/tree
~~~~~~~~~~~~~

Implementation of the Priority tree data structure.
"""

import heapq

from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar


SNAPSHOT_VERSION = 1
SnapshotRecord = Dict[str, Any]
SnapshotRecords = Dict[int, SnapshotRecord]
ParentMap = Dict[int, Optional[int]]
ChildrenMap = Dict[int, List[int]]
QueueMap = Dict[int, List[Tuple[int, int]]]
PriorityTreeType = TypeVar("PriorityTreeType", bound="PriorityTree")


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
    A priority tree snapshot is malformed.
    """

    pass


class UnsupportedSnapshotVersionError(InvalidSnapshotError):
    """
    A priority tree snapshot uses an unknown format version.
    """

    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise InvalidSnapshotError(message)


def _require_exact_type(value: Any, expected_type: type, name: str) -> None:
    if type(value) is not expected_type:
        raise InvalidSnapshotError("%s has an invalid type" % name)


def _snapshot_stream(record: Any, name: str) -> Dict[str, Any]:
    _require_exact_type(record, dict, name)
    return record


def _required_int(record: Dict[str, Any], name: str) -> int:
    value = record.get(name)
    _require(type(value) is int, "%s must be an integer" % name)
    return value


def _validate_queue_entry(entry: Any, index: int) -> Tuple[int, int]:
    entry_name = "child_queue entry %d" % index
    _require_exact_type(entry, dict, entry_name)
    _require(
        set(entry.keys()) == {"stream_id", "level"},
        "%s has invalid fields" % entry_name,
    )

    stream_id = entry["stream_id"]
    level = entry["level"]
    _require(type(stream_id) is int, "%s stream_id must be an integer" % entry_name)
    _require(type(level) is int and level >= 0, "%s level is invalid" % entry_name)
    return level, stream_id


def _snapshot_header(snapshot: SnapshotRecord) -> int:
    version = snapshot.get("version")
    if "version" not in snapshot:
        raise InvalidSnapshotError("Snapshot is missing a format version")
    if type(version) is not int or version != SNAPSHOT_VERSION:
        raise UnsupportedSnapshotVersionError("Unsupported snapshot version")

    _require(
        set(snapshot.keys()) == {"version", "maximum_streams", "streams"},
        "Snapshot has invalid fields",
    )
    maximum_streams = snapshot["maximum_streams"]
    _require(
        type(maximum_streams) is int and maximum_streams > 0,
        "maximum_streams must be a positive integer",
    )
    _require_exact_type(snapshot["streams"], list, "streams")
    return maximum_streams


def _snapshot_records(stream_records: List[Any]) -> SnapshotRecords:
    records: SnapshotRecords = {}
    required_fields = {
        "stream_id",
        "parent_id",
        "weight",
        "active",
        "deficit",
        "last_weight",
        "children",
        "child_queue",
    }

    for index, raw_record in enumerate(stream_records):
        record = _snapshot_stream(raw_record, "stream %d" % index)
        _require(
            set(record.keys()) == required_fields,
            "stream %d has invalid fields" % index,
        )
        stream_id = _required_int(record, "stream_id")
        if stream_id in records:
            raise DuplicateStreamError("Stream %d already in snapshot" % stream_id)
        records[stream_id] = record

    _require(0 in records, "Snapshot is missing root stream 0")
    return records


def _record_parent(stream_id: int, record: SnapshotRecord, records: SnapshotRecords) -> Optional[int]:
    parent_id = record["parent_id"]
    if stream_id == 0:
        _require(parent_id is None, "Root stream must not have a parent")
        return None

    _require(type(parent_id) is int, "Stream %d has an invalid parent" % stream_id)
    _require(parent_id in records, "Stream %d has an unknown parent" % stream_id)
    _require(parent_id != stream_id, "Stream %d must not depend on itself" % stream_id)
    return parent_id


def _record_children(stream_id: int, record: SnapshotRecord, records: SnapshotRecords) -> List[int]:
    raw_children = record["children"]
    _require_exact_type(raw_children, list, "Stream %d children" % stream_id)
    children: List[int] = []
    for child_id in raw_children:
        _require(type(child_id) is int, "Stream %d has an invalid child ID" % stream_id)
        _require(child_id in records, "Stream %d has an unknown child" % stream_id)
        _require(child_id != 0, "Root stream must not be a child")
        _require(
            child_id not in children,
            "Stream %d is listed as a child twice" % stream_id,
        )
        children.append(child_id)
    return children


def _record_queue(stream_id: int, record: SnapshotRecord, children: List[int]) -> List[Tuple[int, int]]:
    raw_queue = record["child_queue"]
    _require_exact_type(raw_queue, list, "Stream %d child_queue" % stream_id)
    queue = [_validate_queue_entry(entry, index) for index, entry in enumerate(raw_queue)]
    queue_ids = [child_id for _, child_id in queue]

    _require(
        len(queue_ids) == len(set(queue_ids)),
        "Stream %d has a duplicated queue entry" % stream_id,
    )
    _require(
        set(queue_ids) == set(children),
        "Stream %d queue does not match its children" % stream_id,
    )

    for position, entry in enumerate(queue):
        left = position * 2 + 1
        right = left + 1
        if left < len(queue):
            _require(entry <= queue[left], "Stream %d queue is not a valid heap" % stream_id)
        if right < len(queue):
            _require(entry <= queue[right], "Stream %d queue is not a valid heap" % stream_id)
    return queue


def _validated_snapshot_graph(
    records: SnapshotRecords,
) -> Tuple[ParentMap, ChildrenMap, QueueMap]:
    parents: ParentMap = {}
    children_by_id: ChildrenMap = {}
    queues_by_id: QueueMap = {}

    for stream_id, record in records.items():
        parent_id = _record_parent(stream_id, record, records)
        weight = record["weight"]
        if type(weight) is not int:
            raise BadWeightError("Stream weight should be an integer")
        if not (1 <= weight <= 256):
            raise BadWeightError(
                "Stream weight must be between 1 and 256 (inclusive)"
            )

        _require(
            type(record["active"]) is bool,
            "Stream %d has an invalid active flag" % stream_id,
        )
        if stream_id == 0:
            if record["weight"] != 1:
                raise BadWeightError(
                    "Stream weight must be between 1 and 256 (inclusive)"
                )
            _require(record["active"] is False, "Root stream must be blocked")

        deficit = _required_int(record, "deficit")
        if stream_id == 0:
            _require(deficit == 0, "Stream 0 has an invalid deficit")
        else:
            _require(0 <= deficit < weight, "Stream %d has an invalid deficit" % stream_id)

        last_weight = _required_int(record, "last_weight")
        _require(last_weight >= 0, "Stream %d has an invalid last weight" % stream_id)

        children = _record_children(stream_id, record, records)
        queue = _record_queue(stream_id, record, children)
        parents[stream_id] = parent_id
        children_by_id[stream_id] = children
        queues_by_id[stream_id] = queue

    return parents, children_by_id, queues_by_id


def _validate_edge_consistency(parents: ParentMap, children_by_id: ChildrenMap) -> None:
    for stream_id, children in children_by_id.items():
        for child_id in children:
            _require(
                parents[child_id] == stream_id,
                "Stream %d has an inconsistent parent edge" % child_id,
            )

    for stream_id, parent_id in parents.items():
        if stream_id != 0:
            _require(
                parent_id is not None
                and stream_id in children_by_id[parent_id],
                "Stream %d is missing from its parent's children" % stream_id,
            )


def _validate_tree_shape(
    records: SnapshotRecords,
    children_by_id: ChildrenMap,
    maximum_streams: int,
) -> None:
    states: Dict[int, str] = {}

    for start in records:
        if states.get(start) == "done":
            continue

        stack: List[Tuple[int, bool]] = [(start, False)]
        while stack:
            stream_id, exiting = stack.pop()
            state = states.get(stream_id)
            if exiting:
                states[stream_id] = "done"
                continue
            if state == "done":
                continue
            if state == "visiting":
                raise PriorityLoop("Snapshot contains a priority loop")

            states[stream_id] = "visiting"
            stack.append((stream_id, True))
            stack.extend(
                (child_id, False) for child_id in children_by_id[stream_id]
            )

    visited = set()
    to_visit = [0]
    while to_visit:
        stream_id = to_visit.pop()
        visited.add(stream_id)
        to_visit.extend(children_by_id[stream_id])
    _require(visited == set(records), "Snapshot contains an unreachable stream")
    if len(records) > maximum_streams:
        raise TooManyStreamsError("Snapshot exceeds maximum_streams")


def _build_priority_tree(
    records: SnapshotRecords,
    parents: ParentMap,
    children_by_id: ChildrenMap,
    queues_by_id: QueueMap,
    maximum_streams: int,
    tree_class: Type[PriorityTreeType],
) -> PriorityTreeType:
    streams: Dict[int, Stream] = {}
    for stream_id, record in records.items():
        stream = object.__new__(Stream)
        stream.stream_id = stream_id
        stream._weight = record["weight"]
        stream.children = []
        stream.parent = None
        stream.child_queue = []
        stream.active = record["active"]
        stream.last_weight = record["last_weight"]
        stream._deficit = record["deficit"]
        streams[stream_id] = stream

    for stream_id, stream in streams.items():
        parent_id = parents[stream_id]
        if parent_id is not None:
            stream.parent = streams[parent_id]
        stream.children = [streams[child_id] for child_id in children_by_id[stream_id]]
        stream.child_queue = [
            (level, streams[child_id]) for level, child_id in queues_by_id[stream_id]
        ]

    tree = object.__new__(tree_class)
    tree._root_stream = streams[0]
    tree._streams = streams
    tree._maximum_streams = maximum_streams
    return tree


def _build_tree_from_snapshot(
    snapshot: Any,
    tree_class: Optional[Type[PriorityTreeType]] = None,
) -> PriorityTreeType:
    _require_exact_type(snapshot, dict, "Snapshot")
    if tree_class is None:
        tree_class = PriorityTree
    maximum_streams = _snapshot_header(snapshot)
    records = _snapshot_records(snapshot["streams"])
    parents, children_by_id, queues_by_id = _validated_snapshot_graph(records)
    _validate_edge_consistency(parents, children_by_id)
    _validate_tree_shape(records, children_by_id, maximum_streams)
    return _build_priority_tree(
        records,
        parents,
        children_by_id,
        queues_by_id,
        maximum_streams,
        tree_class,
    )


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

    def export_state(self) -> Dict[str, Any]:
        """
        Return a versioned snapshot containing only JSON-compatible data.

        This method does not advance the scheduler.
        """
        streams = []
        for stream_id in sorted(self._streams):
            stream = self._streams[stream_id]
            streams.append(
                {
                    "stream_id": stream.stream_id,
                    "parent_id": (
                        None if stream.parent is None else stream.parent.stream_id
                    ),
                    "weight": stream.weight,
                    "active": stream.active,
                    "deficit": stream._deficit,
                    "last_weight": stream.last_weight,
                    "children": [child.stream_id for child in stream.children],
                    "child_queue": [
                        {"stream_id": child.stream_id, "level": level}
                        for level, child in stream.child_queue
                    ],
                }
            )

        return {
            "version": SNAPSHOT_VERSION,
            "maximum_streams": self._maximum_streams,
            "streams": streams,
        }

    @classmethod
    def from_state(
        cls: Type[PriorityTreeType],
        snapshot: Dict[str, Any],
    ) -> PriorityTreeType:
        """
        Build a new tree from a snapshot returned by :meth:`export_state`.
        """
        return _build_tree_from_snapshot(snapshot, cls)

    def restore_state(self, snapshot: Dict[str, Any]) -> None:
        """
        Replace this tree's state with a fully validated snapshot.

        The existing tree is not modified if the snapshot is invalid.
        """
        restored_tree = _build_tree_from_snapshot(snapshot)
        if len(restored_tree._streams) > self._maximum_streams:
            raise TooManyStreamsError(
                "Refusing to insert %d streams into priority tree at once"
                % (self._maximum_streams + 1)
            )
        self._root_stream = restored_tree._root_stream
        self._streams = restored_tree._streams
        self._maximum_streams = restored_tree._maximum_streams

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
