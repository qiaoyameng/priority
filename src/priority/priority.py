# -*- coding: utf-8 -*-
"""
priority/tree
~~~~~~~~~~~~~

Implementation of the Priority tree data structure.
"""

import heapq
from fractions import Fraction

from typing import Dict, List, Optional, Tuple, Union


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


class TooManyPlaceholdersError(PriorityError):
    """
    An attempt was made to insert more placeholder streams than the priority
    tree is configured to retain.
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
        self._effective_weight: Union[int, Fraction] = weight
        self.children: List[Stream] = []
        self.parent: Optional[Stream] = None
        self.child_queue: List[Tuple[Union[int, Fraction], Stream]] = []
        self.active = True
        self.placeholder = placeholder
        self.last_weight: Union[int, Fraction] = 0
        self._deficit: Union[int, Fraction] = 0

    @property
    def effective_weight(self) -> Union[int, Fraction]:
        """
        The weight used by the scheduler. This is the declared stream weight
        unless placeholder compression has redistributed a parent's share.
        """
        return self._effective_weight

    @effective_weight.setter
    def effective_weight(self, weight: Union[int, Fraction]) -> None:
        self._effective_weight = weight

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

        new_queue: List[Tuple[Union[int, Fraction], Stream]] = []

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
                level += (256 + child._deficit) // child.effective_weight
                child._deficit = (256 + child._deficit) % child.effective_weight
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

    .. versionchanged:: 2.1.0
       Added ``maximum_placeholders`` keyword argument and placeholder
       compression.

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

    :param maximum_placeholders: The maximum number of automatically inserted
        blocked placeholder nodes that may be retained before one is
        compressed. Placeholders are separate from explicitly registered
        streams and do not consume ``maximum_streams`` capacity.
    :type maximum_placeholders: ``int``
    """

    def __init__(
        self,
        maximum_streams: int = 1000,
        maximum_placeholders: int = 100,
    ) -> None:
        # This flat array keeps hold of all the streams that are logically
        # dependent on stream 0.
        self._root_stream = Stream(stream_id=0, weight=1)
        self._root_stream.active = False
        self._all_streams = {0: self._root_stream}
        self._streams = {0: self._root_stream}
        self._placeholders: Dict[int, Stream] = {}

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

    def _get_or_insert_parent(self, parent_stream_id: int) -> Stream:
        """
        When inserting or reprioritizing a stream it is possible to make it
        dependent on a stream that is no longer in the tree. In this situation,
        rather than bail out, we insert a blocked placeholder node. Such a node
        is accounted for separately from a registered stream.
        """
        try:
            return self._all_streams[parent_stream_id]
        except KeyError:
            if len(self._placeholders) >= self._maximum_placeholders:
                self._compress_placeholder()

            placeholder = Stream(
                stream_id=parent_stream_id,
                weight=16,
                placeholder=True,
            )
            placeholder.active = False
            self._root_stream.add_child(placeholder)
            self._all_streams[parent_stream_id] = placeholder
            self._placeholders[parent_stream_id] = placeholder
            return placeholder

    def _compress_placeholder(self) -> int:
        """
        Flatten one blocked placeholder node. Childless placeholders are
        preferred because their removal requires no weight redistribution.
        """
        candidate = None

        for placeholder in self._placeholders.values():
            if placeholder.active or placeholder.children:
                continue
            if candidate is None or placeholder.stream_id < candidate.stream_id:
                candidate = placeholder

        if candidate is None:
            for placeholder in self._placeholders.values():
                if placeholder.active:
                    continue
                if candidate is None or placeholder.stream_id < candidate.stream_id:
                    candidate = placeholder

        if candidate is None:
            raise TooManyPlaceholdersError(
                "Refusing to insert more than %d placeholder streams into the "
                "priority tree at once" % self._maximum_placeholders
            )

        return self._flatten_placeholder(candidate)

    def _flatten_placeholder(self, placeholder: Stream) -> int:
        """
        Move a placeholder's children to its parent while preserving the
        children's aggregate and relative scheduling shares.
        """
        parent = placeholder.parent
        if parent is None:  # pragma: no cover
            raise PriorityError("Placeholder has no parent")

        children = list(placeholder.children)
        parent.remove_child(placeholder, strip_children=False)

        if children:
            total_weight = sum(
                (child.effective_weight for child in children),
                Fraction(0),
            )
            scale = placeholder.effective_weight / total_weight

            for child in children:
                child.effective_weight *= scale
                child._deficit = 0
                parent.add_child(child)

        placeholder.children = []
        placeholder.child_queue = []
        placeholder.parent = None

        del self._all_streams[placeholder.stream_id]
        del self._placeholders[placeholder.stream_id]
        return placeholder.stream_id

    def _discard_empty_placeholder(self, stream: Optional[Stream]) -> None:
        """
        Remove a placeholder once it no longer represents any dependency.
        """
        if stream is None or not stream.placeholder or stream.children:
            return

        parent = stream.parent
        if parent is not None:
            parent.remove_child(stream, strip_children=False)

        stream.child_queue = []
        stream.parent = None
        del self._all_streams[stream.stream_id]
        del self._placeholders[stream.stream_id]

    def _move_stream(
        self,
        current_stream: Stream,
        depends_on: Optional[int],
        weight: int,
        exclusive: bool,
    ) -> None:
        """
        Move an existing registered or newly promoted stream to a new parent.
        """
        if depends_on:
            if depends_on == current_stream.stream_id:
                raise PriorityLoop(
                    "Stream %d must not depend on itself" % current_stream.stream_id
                )

            new_parent = self._get_or_insert_parent(depends_on)
            cycle = _stream_cycle(new_parent, current_stream)
        else:
            new_parent = self._root_stream
            cycle = False

        current_stream.weight = weight
        current_stream.effective_weight = weight

        if cycle:
            old_parent = new_parent.parent
            new_parent.parent.remove_child(  # type: ignore[union-attr]
                new_parent,
            )
            current_stream.parent.add_child(  # type: ignore[union-attr]
                new_parent,
            )
            self._discard_empty_placeholder(old_parent)

        previous_parent = current_stream.parent
        previous_parent.remove_child(  # type: ignore[union-attr]
            current_stream, strip_children=False
        )
        self._discard_empty_placeholder(previous_parent)

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
        if stream_id in self._streams:
            raise DuplicateStreamError("Stream %d already in tree" % stream_id)

        if (len(self._streams) + 1) > self._maximum_streams:
            raise TooManyStreamsError(
                "Refusing to insert %d streams into priority tree at once"
                % (self._maximum_streams + 1)
            )

        if depends_on == stream_id:
            raise PriorityLoop("Stream %d must not depend on itself." % stream_id)

        depends_on = depends_on if depends_on else 0
        stream = Stream(stream_id, weight)
        existing = self._all_streams.get(stream_id)

        if existing is not None:
            placeholder = existing
            self._placeholders.pop(stream_id)
            placeholder.placeholder = False
            placeholder.active = True
            self._streams[stream_id] = placeholder
            self._move_stream(
                placeholder,
                depends_on,
                weight,
                exclusive,
            )
            return

        parent = self._get_or_insert_parent(depends_on)
        if exclusive:
            parent.add_child_exclusive(stream)
        else:
            parent.add_child(stream)
        self._all_streams[stream_id] = stream
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

        self._move_stream(current_stream, depends_on, weight, exclusive)

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
        child.parent = None
        del self._all_streams[stream_id]

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
