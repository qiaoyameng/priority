# -*- coding: utf-8 -*-
"""
priority/tree
~~~~~~~~~~~~~

Implementation of the Priority tree data structure.
"""

import heapq

from typing import List, Tuple, Optional, Set


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


class TooManyPlaceholdersError(TooManyStreamsError):
    """
    The placeholder budget is exhausted and no placeholder can be compressed.
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

    def __init__(
        self,
        stream_id: int,
        weight: int = 16,
        placeholder: bool = False,
    ) -> None:
        self.stream_id = stream_id
        self.weight = weight
        self.children: List[Stream] = []
        self.parent: Optional[Stream] = None
        self.child_queue: List[Tuple[int, Stream]] = []
        self.placeholder = placeholder
        self.active = not placeholder
        self.last_weight = 0
        self._effective_weight = weight
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
                level += (256 + child._deficit) // child._effective_weight
                child._deficit = (256 + child._deficit) % child._effective_weight
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
    :param maximum_placeholders: The maximum number of placeholder nodes that
        may be retained at once. Placeholders are created when a stream
        depends on an ID that has not been registered. They have a separate
        budget from registered streams. When this budget is reached, the tree
        will first remove a childless placeholder, or compress one non-empty
        placeholder. Defaults to 1000.
    :type maximum_placeholders: ``int``

    A placeholder is a blocked, internal node whose ID has only appeared as
    an absent parent. It is not a registered stream, even if it later becomes
    blocked. A registered stream that is temporarily blocked is never
    compressed.

    When a non-empty placeholder with weight ``W`` is compressed, it must
    have a blocked parent. Its children are moved to that parent. If a child
    previously had effective weight ``w`` and the immediate children had
    total effective weight ``S``, the child's scheduling weight becomes the
    integer nearest ``W * w / S`` (exact halves round up), clamped to the
    range 1–256. The child's explicitly assigned ``weight`` is unchanged.
    This keeps the compressed subtree's aggregate weight close to ``W`` and
    preserves the children's relative weights as closely as HTTP/2's integer
    weights allow. All active streams below the placeholder remain reachable
    through their new parent.

    Inserting or explicitly unblocking a stream with the same ID as a
    placeholder promotes that node into a registered, active stream.
    """

    def __init__(
        self,
        maximum_streams: int = 1000,
        maximum_placeholders: int = 1000,
    ) -> None:
        # This flat array keeps hold of all the streams that are logically
        # dependent on stream 0.
        self._root_stream = Stream(stream_id=0, weight=1)
        self._root_stream.active = False
        self._streams = {0: self._root_stream}
        self._registered_stream_ids: Set[int] = {0}
        self._placeholder_stream_ids: Set[int] = set()

        if not isinstance(maximum_streams, int):
            raise TypeError("maximum_streams must be an int.")
        if maximum_streams <= 0:
            raise ValueError("maximum_streams must be a positive integer.")
        self._maximum_streams = maximum_streams

        if not isinstance(maximum_placeholders, int):
            raise TypeError("maximum_placeholders must be an int.")
        if maximum_placeholders <= 0:
            raise ValueError("maximum_placeholders must be a positive integer.")
        self._maximum_placeholders = maximum_placeholders

    def _get_or_insert_parent(
        self,
        parent_stream_id: int,
        protected_stream_id: Optional[int] = None,
    ) -> Stream:
        """
        When inserting or reprioritizing a stream it is possible to make it
        dependent on a stream that has not been registered. In this
        situation, rather than bail out, we insert a blocked placeholder
        node.
        """
        try:
            return self._streams[parent_stream_id]
        except KeyError:
            self._ensure_placeholder_capacity(protected_stream_id)
            placeholder = Stream(parent_stream_id, placeholder=True)
            self._streams[parent_stream_id] = placeholder
            self._placeholder_stream_ids.add(parent_stream_id)
            self._root_stream.add_child(placeholder)
            return placeholder

    def _ensure_placeholder_capacity(
        self,
        protected_stream_id: Optional[int] = None,
    ) -> None:
        if len(self._placeholder_stream_ids) < self._maximum_placeholders:
            return

        if self._compress_placeholder(protected_stream_id) is None:
            raise TooManyPlaceholdersError(
                "Refusing to retain more than %d placeholder streams"
                % self._maximum_placeholders
            )

    @staticmethod
    def _compressed_child_effective_weight(
        placeholder_weight: int,
        child_weight: int,
        total_child_weight: int,
    ) -> int:
        scaled_weight = (
            2 * placeholder_weight * child_weight + total_child_weight
        ) // (2 * total_child_weight)
        return min(256, max(1, scaled_weight))

    def _compress_placeholder(
        self,
        protected_stream_id: Optional[int] = None,
    ) -> Optional[int]:
        """
        Compress one placeholder and return its ID, if an eligible node
        exists. Childless placeholders are removed first. A non-empty
        placeholder is eligible only when its parent is blocked, which keeps
        its active descendants reachable after compression.
        """
        empty_candidate = None
        non_empty_candidate = None

        for placeholder_id in sorted(self._placeholder_stream_ids):
            if placeholder_id == protected_stream_id:
                continue

            placeholder = self._streams[placeholder_id]
            if not placeholder.children:
                if empty_candidate is None:
                    empty_candidate = placeholder
                continue

            parent = placeholder.parent
            if (
                non_empty_candidate is None
                and parent is not None
                and not parent.active
            ):
                non_empty_candidate = placeholder

        candidate = empty_candidate or non_empty_candidate

        if candidate is None:
            return None

        parent = candidate.parent
        children = list(candidate.children)
        total_child_weight = sum(child._effective_weight for child in children)

        for child in children:
            child_weight = self._compressed_child_effective_weight(
                candidate.weight,
                child._effective_weight,
                total_child_weight,
            )
            candidate.remove_child(child, strip_children=False)
            child._effective_weight = child_weight
            parent.add_child(child)  # type: ignore[union-attr]

        parent.remove_child(  # type: ignore[union-attr]
            candidate,
            strip_children=False,
        )
        placeholder_id = candidate.stream_id
        del self._streams[placeholder_id]
        self._placeholder_stream_ids.remove(placeholder_id)
        return placeholder_id

    def _promote_placeholder(
        self,
        placeholder: Stream,
        depends_on: int,
        weight: int,
        exclusive: bool,
    ) -> None:
        if len(self._registered_stream_ids) >= self._maximum_streams:
            raise TooManyStreamsError(
                "Refusing to insert %d streams into priority tree at once"
                % (self._maximum_streams + 1)
            )

        placeholder_id = placeholder.stream_id
        placeholder.placeholder = False
        self._placeholder_stream_ids.remove(placeholder_id)
        self._registered_stream_ids.add(placeholder_id)

        new_parent = self._get_or_insert_parent(depends_on, placeholder_id)
        placeholder.weight = weight
        self._move_stream(placeholder, new_parent, weight, exclusive)
        placeholder.active = True

    def _move_stream(
        self,
        current_stream: Stream,
        new_parent: Stream,
        weight: int,
        exclusive: bool,
    ) -> None:
        current_stream.weight = weight
        current_stream._effective_weight = weight
        cycle = (
            False
            if new_parent.stream_id == 0
            else _stream_cycle(new_parent, current_stream)
        )

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
        existing_stream = self._streams.get(stream_id)
        if existing_stream is not None and existing_stream.placeholder:
            if not depends_on:
                depends_on = 0
            elif depends_on == stream_id:
                raise PriorityLoop("Stream %d must not depend on itself." % stream_id)

            self._promote_placeholder(
                existing_stream,
                depends_on,
                weight,
                exclusive,
            )
            return

        if stream_id in self._streams:
            raise DuplicateStreamError("Stream %d already in tree" % stream_id)

        if len(self._registered_stream_ids) >= self._maximum_streams:
            raise TooManyStreamsError(
                "Refusing to insert %d streams into priority tree at once"
                % (self._maximum_streams + 1)
            )

        stream = Stream(stream_id, weight)

        if not depends_on:
            depends_on = 0
        elif depends_on == stream_id:
            raise PriorityLoop("Stream %d must not depend on itself." % stream_id)

        parent = self._get_or_insert_parent(depends_on, stream_id)
        if exclusive:
            self._exclusive_insert(parent, stream)
        else:
            parent.add_child(stream)

        self._streams[stream_id] = stream
        self._registered_stream_ids.add(stream_id)

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

            new_parent = self._get_or_insert_parent(depends_on, stream_id)
        else:
            new_parent = self._streams[0]
        self._move_stream(current_stream, new_parent, weight, exclusive)

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
        self._registered_stream_ids.discard(stream_id)
        self._placeholder_stream_ids.discard(stream_id)

    def block(self, stream_id: int) -> None:
        """
        Marks a given stream as blocked, with no data to send.

        :param stream_id: The ID of the stream to block.
        """
        if stream_id == 0:
            raise PseudoStreamError("Cannot block stream 0")

        try:
            stream = self._streams[stream_id]
        except KeyError:
            raise MissingStreamError("Stream %d not in tree" % stream_id)
        stream.active = False

    def unblock(self, stream_id: int) -> None:
        """
        Marks a given stream as unblocked, with more data to send.

        :param stream_id: The ID of the stream to unblock.
        """
        if stream_id == 0:
            raise PseudoStreamError("Cannot unblock stream 0")

        try:
            stream = self._streams[stream_id]
        except KeyError:
            raise MissingStreamError("Stream %d not in tree" % stream_id)

        if stream.placeholder:
            if len(self._registered_stream_ids) >= self._maximum_streams:
                raise TooManyStreamsError(
                    "Refusing to insert %d streams into priority tree at once"
                    % (self._maximum_streams + 1)
                )
            stream.placeholder = False
            self._placeholder_stream_ids.remove(stream_id)
            self._registered_stream_ids.add(stream_id)

        stream.active = True

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
