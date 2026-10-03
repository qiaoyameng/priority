# -*- coding: utf-8 -*-
"""
test_priority
~~~~~~~~~~~~~

Tests for the Priority trees
"""

import operator
import collections
import itertools
import json

import pytest

from hypothesis import given, settings
from hypothesis.stateful import invariant, RuleBasedStateMachine, rule
from hypothesis.strategies import integers, lists, tuples, sampled_from

import priority

from typing import Iterable, List, Dict, Any


STREAMS_AND_WEIGHTS = lists(
    elements=tuples(integers(min_value=1), integers(min_value=1, max_value=255)),
    unique_by=operator.itemgetter(0),
)

BLOCKED_AND_ACTIVE = lists(
    elements=sampled_from([1, 3, 5, 7, 9, 11]),
    unique=True,
).map(lambda blocked: (blocked, active_readme_streams_from_filter(blocked)))

UNBLOCKED_AND_ACTIVE = lists(
    elements=sampled_from([1, 3, 5, 7, 9, 11]),
    unique=True,
).map(
    lambda unblocked: (
        unblocked,
        active_readme_streams_from_filter(unblocked, blocked=False),
    )
)


def readme_tree():
    """
    Provide a tree configured as the one in the readme.
    """
    p = priority.PriorityTree()
    p.insert_stream(stream_id=1)
    p.insert_stream(stream_id=3)
    p.insert_stream(stream_id=5, depends_on=1)
    p.insert_stream(stream_id=7, weight=32)
    p.insert_stream(stream_id=9, depends_on=7, weight=8)
    p.insert_stream(stream_id=11, depends_on=7, exclusive=True)
    return p


def active_readme_streams_from_filter(
    filtered: Iterable[int],
    blocked: bool = True,
) -> List[int]:
    """
    Given a collection of filtered streams, determine which ones are active.
    This applies only to the readme tree at this time, though in future it
    should be possible to apply this to an arbitrary tree.

    If ``blocked`` is ``True``, the filter is a set of blocked streams. If
    ``False``, it's a collection of unblocked streams.
    """
    tree = {
        1: {
            5: {},
        },
        3: {},
        7: {
            11: {
                9: {},
            },
        },
    }
    filtered = set(filtered)

    def get_expected(tree: Dict[Any, Any]) -> List[int]:
        expected = []

        for stream_id in tree:
            if stream_id not in filtered and blocked:
                expected.append(stream_id)
            elif stream_id in filtered and not blocked:
                expected.append(stream_id)
            else:
                expected.extend(get_expected(tree[stream_id]))

        return expected

    return get_expected(tree)


class TestStream:
    def test_stream_repr(self):
        """
        The stream representation renders according to the README.
        """
        s = priority.Stream(stream_id=80, weight=16)
        assert repr(s) == "Stream<id=80, weight=16>"

    @given(STREAMS_AND_WEIGHTS)
    def test_streams_are_well_ordered(self, streams_and_weights):
        """
        Streams are ordered by their stream ID.
        """
        stream_list = [
            priority.Stream(stream_id=s, weight=w) for s, w in streams_and_weights
        ]
        stream_list = sorted(stream_list)
        streams_by_id = [stream.stream_id for stream in stream_list]
        assert sorted(streams_by_id) == streams_by_id

    @given(
        integers(min_value=1, max_value=2**24),
        integers(min_value=1, max_value=2**24),
    )
    def test_stream_ordering(self, a, b):
        """
        Two streams are well ordered based on their stream ID.
        """
        s1 = priority.Stream(stream_id=a, weight=16)
        s2 = priority.Stream(stream_id=b, weight=32)

        assert (s1 < s2) == (a < b)
        assert (s1 <= s2) == (a <= b)
        assert (s1 > s2) == (a > b)
        assert (s1 >= s2) == (a >= b)
        assert (s1 == s2) == (a == b)
        assert (s1 != s2) == (a != b)


class TestPriorityTreeManual:
    """
    These tests manually confirm that the PriorityTree output is correct. They
    use the PriorityTree given in the README and confirm that it outputs data
    as expected.

    If possible, I'd like to eventually replace these tests with
    Hypothesis-based ones for the same data, but getting Hypothesis to generate
    useful data in this case is going to be quite tricky.
    """

    @given(BLOCKED_AND_ACTIVE)
    def test_priority_tree_initially_outputs_all_stream_ids(self, blocked_expected):
        """
        The first iterations of the priority tree initially output the active
        streams, in order of stream ID, regardless of weight.
        """
        tree = readme_tree()
        blocked = blocked_expected[0]
        expected = blocked_expected[1]

        for stream_id in blocked:
            tree.block(stream_id)

        result = [next(tree) for _ in range(len(expected))]
        assert expected == result

    @given(UNBLOCKED_AND_ACTIVE)
    def test_priority_tree_blocking_is_isomorphic(self, allowed_expected):
        """
        Blocking everything and then unblocking certain ones has the same
        effect as blocking specific streams.
        """
        tree = readme_tree()
        allowed = allowed_expected[0]
        expected = allowed_expected[1]

        for stream_id in range(1, 12, 2):
            tree.block(stream_id)

        for stream_id in allowed:
            tree.unblock(stream_id)

        result = [next(tree) for _ in range(len(expected))]
        assert expected == result

    @given(BLOCKED_AND_ACTIVE)
    def test_removing_items_behaves_similarly_to_blocking(self, blocked_expected):
        """
        From the perspective of iterating over items, removing streams should
        have the same effect as blocking them, except that the ordering
        changes. Because the ordering is not important, don't test for it.
        """
        tree = readme_tree()
        blocked = blocked_expected[0]
        expected = set(blocked_expected[1])

        for stream_id in blocked:
            tree.remove_stream(stream_id)

        result = set(next(tree) for _ in range(len(expected)))
        assert expected == result

    def test_priority_tree_raises_deadlock_error_if_all_blocked(self):
        """
        Assuming all streams are blocked and none can progress, asking for the
        one with the next highest priority fires a DeadlockError.
        """
        tree = readme_tree()
        for stream_id in range(1, 12, 2):
            tree.block(stream_id)

        with pytest.raises(priority.DeadlockError):
            next(tree)

    @pytest.mark.parametrize(
        "stream,new_parent,exclusive,weight,blocked,result",
        [
            (1, 3, False, 16, [], [3, 7, 7, 3, 7, 7, 3, 7, 7]),
            (1, 5, False, 16, [], [3, 5, 7, 7, 3, 5, 7, 7, 3]),
            (1, 5, False, 16, [5], [3, 1, 7, 7, 3, 1, 7, 7, 3]),
            (5, 7, False, 16, [7, 1], [3, 5, 11, 3, 5, 11, 3, 5, 11]),
            (11, None, False, 16, [], [1, 3, 7, 11, 7, 1, 3, 7, 11]),
            (11, None, False, 16, [11], [1, 3, 7, 9, 7, 1, 3, 7, 9]),
            (7, 9, False, 16, [], [1, 3, 9, 1, 3, 1, 3, 9, 1]),
            (7, 1, True, 16, [], [1, 3, 1, 3, 1, 3, 1, 3, 1]),
            (7, 1, True, 16, [1], [7, 3, 7, 3, 7, 3, 7, 3, 7]),
            (7, 1, True, 16, [1, 7], [5, 3, 11, 3, 5, 3, 11, 3, 5]),
            (1, 0, False, 32, [], [1, 3, 7, 1, 7, 1, 3, 7, 1]),
            (1, 0, True, 32, [], [1, 1, 1, 1, 1, 1, 1, 1, 1]),
            (1, 0, True, 32, [1], [3, 5, 7, 7, 3, 5, 7, 7, 3]),
            (1, None, True, 32, [], [1, 1, 1, 1, 1, 1, 1, 1, 1]),
            (1, None, True, 32, [1], [3, 5, 7, 7, 3, 5, 7, 7, 3]),
        ],
    )
    def test_can_reprioritize_a_stream(
        self, stream, new_parent, exclusive, weight, blocked, result
    ):
        """
        Reprioritizing streams adjusts the outputs of the tree.
        """
        t = readme_tree()

        for s in blocked:
            t.block(s)

        t.reprioritize(
            stream_id=stream,
            depends_on=new_parent,
            weight=weight,
            exclusive=exclusive,
        )

        actual_result = [next(t) for _ in range(len(result))]
        assert actual_result == result

    def test_priority_tree_raises_error_inserting_duplicate(self):
        """
        Attempting to insert a stream that is already in the tree raises a
        DuplicateStreamError
        """
        p = priority.PriorityTree()
        p.insert_stream(1)

        with pytest.raises(priority.DuplicateStreamError):
            p.insert_stream(1)

    def test_priority_raises_good_errors_for_missing_streams(self):
        """
        Attempting operations on absent streams raises a MissingStreamError.
        """
        p = priority.PriorityTree()
        p.insert_stream(1)

        with pytest.raises(priority.MissingStreamError):
            p.reprioritize(3)

        with pytest.raises(priority.MissingStreamError):
            p.block(3)

        with pytest.raises(priority.MissingStreamError):
            p.unblock(3)

        with pytest.raises(priority.MissingStreamError):
            p.remove_stream(3)

    def test_priority_raises_good_errors_for_zero_stream(self):
        """
        Attempting operations on stream 0 raises a PseudoStreamError.
        """
        p = priority.PriorityTree()
        p.insert_stream(1)

        with pytest.raises(priority.PseudoStreamError):
            p.reprioritize(0)

        with pytest.raises(priority.PseudoStreamError):
            p.block(0)

        with pytest.raises(priority.PseudoStreamError):
            p.unblock(0)

        with pytest.raises(priority.PseudoStreamError):
            p.remove_stream(0)

    @pytest.mark.parametrize("exclusive", [True, False])
    def test_priority_allows_inserting_stream_with_absent_parent(self, exclusive):
        """
        Attemping to insert a stream that depends on a stream that is not in
        the tree automatically inserts the parent with default priority.
        """
        p = priority.PriorityTree()
        p.insert_stream(stream_id=3, depends_on=1, exclusive=exclusive, weight=32)

        # Iterate 10 times to prove that the parent stream starts blocked.
        first_ten_ids = [next(p) for _ in range(0, 10)]
        assert first_ten_ids == [3] * 10

        # Unblock the parent.
        p.unblock(1)

        # Iterate 10 times, expecting only the parent.
        next_ten_ids = [next(p) for _ in range(0, 10)]
        assert next_ten_ids == [1] * 10

        # Insert a new stream into the tree with default priority.
        p.insert_stream(stream_id=5)

        # Iterate 10 more times. Expect the parent, and the new stream, in
        # equal amounts.
        next_ten_ids = [next(p) for _ in range(0, 10)]
        assert next_ten_ids == [5, 1] * 5

    @pytest.mark.parametrize("exclusive", [True, False])
    def test_priority_reprioritizing_stream_with_absent_parent(self, exclusive):
        """
        Attemping to reprioritize a stream to depend on a stream that is not in
        the tree automatically inserts the parent with default priority.
        """
        p = priority.PriorityTree()
        p.insert_stream(stream_id=3)

        p.reprioritize(stream_id=3, depends_on=1, exclusive=exclusive, weight=32)

        # Iterate 10 times to prove that the parent stream starts blocked.
        first_ten_ids = [next(p) for _ in range(0, 10)]
        assert first_ten_ids == [3] * 10

        # Unblock the parent.
        p.unblock(1)

        # Iterate 10 times, expecting only the parent.
        next_ten_ids = [next(p) for _ in range(0, 10)]
        assert next_ten_ids == [1] * 10

        # Insert a new stream into the tree with default priority.
        p.insert_stream(stream_id=5)

        # Iterate 10 more times. Expect the parent, and the new stream, in
        # equal amounts.
        next_ten_ids = [next(p) for _ in range(0, 10)]
        assert next_ten_ids == [5, 1] * 5

    @pytest.mark.parametrize("count", range(2, 10000, 100))
    def test_priority_refuses_to_allow_too_many_streams_in_tree(self, count):
        """
        Attempting to insert more streams than maximum_streams into the tree
        fails.
        """
        p = priority.PriorityTree(maximum_streams=count)

        # This isn't an off-by-one error: stream 0 is in the tree by default.
        for x in range(1, count):
            p.insert_stream(x)

        with pytest.raises(priority.TooManyStreamsError):
            p.insert_stream(x + 1)

    @pytest.mark.parametrize("depends_on", [0, None])
    def test_can_insert_stream_with_exclusive_dependency_on_0(self, depends_on):
        """
        It is acceptable to insert a stream with an exclusive dependency on
        stream 0, both explicitly and implicitly.
        """
        p = priority.PriorityTree()
        p.insert_stream(stream_id=1)
        p.insert_stream(stream_id=3)

        p.insert_stream(stream_id=5, depends_on=depends_on, exclusive=True)

        next_ten_ids = [next(p) for _ in range(0, 10)]
        assert next_ten_ids == [5] * 10

    @pytest.mark.parametrize("weight", [None, 0.5, float("inf"), "priority", object])
    def test_stream_with_non_integer_weight_is_error(self, weight):
        """
        Giving a stream a non-integer weight is rejected.
        """
        p = priority.PriorityTree()
        with pytest.raises(priority.BadWeightError) as err:
            p.insert_stream(stream_id=1, weight=weight)
        assert err.value.args[0] == "Stream weight should be an integer"

        p.insert_stream(stream_id=2)
        with pytest.raises(priority.BadWeightError) as err:
            p.reprioritize(stream_id=2, weight=weight)
        assert err.value.args[0] == "Stream weight should be an integer"

    @pytest.mark.parametrize(
        "weight",
        [
            0,
            257,
            1000,
            -42,
        ],
    )
    def test_stream_with_out_of_bounds_weight_is_error(self, weight):
        """
        Giving a stream an out-of-bounds integer weight is rejected.
        """
        p = priority.PriorityTree()
        with pytest.raises(priority.BadWeightError) as err:
            p.insert_stream(stream_id=1, weight=weight)
        assert (
            err.value.args[0] == "Stream weight must be between 1 and 256 (inclusive)"
        )

        p.insert_stream(stream_id=2)
        with pytest.raises(priority.BadWeightError) as err:
            p.reprioritize(stream_id=2, weight=weight)
        assert (
            err.value.args[0] == "Stream weight must be between 1 and 256 (inclusive)"
        )

    @pytest.mark.parametrize("exclusive", (True, False))
    @pytest.mark.parametrize("stream_id", (1, 5, 20, 32, 256))
    def test_stream_depending_on_self_is_error(self, stream_id, exclusive):
        """
        Inserting a stream that is dependent on itself is rejected.
        """
        p = priority.PriorityTree()
        with pytest.raises(priority.PriorityLoop):
            p.insert_stream(
                stream_id=stream_id, depends_on=stream_id, exclusive=exclusive
            )

    @pytest.mark.parametrize("exclusive", (True, False))
    @pytest.mark.parametrize("stream_id", (1, 5, 20, 32, 256))
    def test_reprioritize_depend_on_self_is_error(self, stream_id, exclusive):
        """
        Reprioritizing a stream to make it dependent on itself is an error.
        """
        p = priority.PriorityTree()
        p.insert_stream(stream_id=stream_id)
        with pytest.raises(priority.PriorityLoop):
            p.reprioritize(
                stream_id=stream_id, depends_on=stream_id, exclusive=exclusive
            )

    @pytest.mark.parametrize("maximum_streams", (None, "foo", object(), 2.0))
    def test_maximum_streams_with_non_int_is_error(self, maximum_streams):
        """
        Creating a PriorityTree with a non-int argument for maximum_streams
        is an error.
        """
        with pytest.raises(TypeError) as err:
            priority.PriorityTree(maximum_streams=maximum_streams)
        assert err.value.args[0] == "maximum_streams must be an int."

    @pytest.mark.parametrize("maximum_streams", (0, -1, -50))
    def test_maximum_streams_with_bad_int_is_error(self, maximum_streams):
        """
        Creating a PriorityTree with a non-positive integer for maximum_streams
        is an error.
        """
        with pytest.raises(ValueError) as err:
            priority.PriorityTree(maximum_streams=maximum_streams)
        assert err.value.args[0] == "maximum_streams must be a positive integer."


class TestPriorityTreeSnapshot:
    def test_snapshot_restores_scheduling_progress(self):
        tree = readme_tree()
        for _ in range(7):
            next(tree)

        snapshot = tree.export_state()
        restored = priority.PriorityTree.from_state(snapshot)

        expected = [next(tree) for _ in range(40)]
        actual = [next(restored) for _ in range(40)]
        assert actual == expected

    def test_snapshot_uses_json_types_and_does_not_advance(self):
        tree = readme_tree()
        for _ in range(7):
            next(tree)

        snapshot = tree.export_state()

        assert json.loads(json.dumps(snapshot)) == snapshot
        assert snapshot == tree.export_state()

        restored = priority.PriorityTree()
        restored.restore_state(snapshot)
        assert next(tree) == next(restored)

    def test_invalid_parent_edge_leaves_receiving_tree_unchanged(self):
        tree = readme_tree()
        snapshot = tree.export_state()
        stream_one = next(
            stream for stream in snapshot["streams"] if stream["stream_id"] == 1
        )
        stream_one["parent_id"] = 999

        receiving_tree = priority.PriorityTree()
        receiving_tree.insert_stream(100)
        before = receiving_tree.export_state()

        with pytest.raises(priority.InvalidSnapshotError):
            receiving_tree.restore_state(snapshot)

        assert receiving_tree.export_state() == before
        assert next(receiving_tree) == 100

    def test_inconsistent_parent_edge_leaves_receiving_tree_unchanged(self):
        snapshot = readme_tree().export_state()
        stream_one = next(
            stream for stream in snapshot["streams"] if stream["stream_id"] == 1
        )
        stream_one["parent_id"] = 3

        receiving_tree = priority.PriorityTree()
        receiving_tree.insert_stream(100)
        before = receiving_tree.export_state()

        with pytest.raises(priority.InvalidSnapshotError):
            receiving_tree.restore_state(snapshot)

        assert receiving_tree.export_state() == before
        assert next(receiving_tree) == 100

    def test_unknown_snapshot_version_is_rejected(self):
        snapshot = readme_tree().export_state()
        snapshot["version"] = 2

        with pytest.raises(priority.UnsupportedSnapshotVersionError):
            priority.PriorityTree.from_state(snapshot)

    def test_snapshot_capacity_is_enforced_before_restore(self):
        tree = priority.PriorityTree(maximum_streams=2)
        tree.insert_stream(1)
        snapshot = tree.export_state()

        receiving_tree = priority.PriorityTree(maximum_streams=3)
        before = receiving_tree.export_state()

        receiving_tree.restore_state(snapshot)
        assert receiving_tree._maximum_streams == 2

        with pytest.raises(priority.TooManyStreamsError):
            receiving_tree.insert_stream(100)

        large_snapshot = readme_tree().export_state()
        limited_tree = priority.PriorityTree(maximum_streams=3)
        limited_before = limited_tree.export_state()
        with pytest.raises(priority.TooManyStreamsError):
            limited_tree.restore_state(large_snapshot)
        assert limited_tree.export_state() == limited_before

    def test_snapshot_preserves_capacity_when_building_new_tree(self):
        tree = priority.PriorityTree(maximum_streams=2)
        tree.insert_stream(1)
        snapshot = tree.export_state()

        restored = priority.PriorityTree.from_state(snapshot)
        assert restored._maximum_streams == 2

        with pytest.raises(priority.TooManyStreamsError):
            restored.insert_stream(100)

    @pytest.mark.parametrize(
        "mutation,error",
        [
            (lambda s: s.pop("version"), priority.InvalidSnapshotError),
            (lambda s: s.__setitem__("version", 0), priority.UnsupportedSnapshotVersionError),
            (lambda s: s.__setitem__("extra", True), priority.InvalidSnapshotError),
            (lambda s: s.__setitem__("maximum_streams", 0), priority.InvalidSnapshotError),
            (lambda s: s["streams"].remove(s["streams"][0]), priority.InvalidSnapshotError),
            (lambda s: s["streams"].append(dict(s["streams"][1])), priority.DuplicateStreamError),
            (lambda s: s["streams"][0].__setitem__("weight", 2), priority.BadWeightError),
            (lambda s: s["streams"][1].__setitem__("weight", 257), priority.BadWeightError),
            (lambda s: s["streams"][1].__setitem__("weight", "16"), priority.BadWeightError),
            (lambda s: s["streams"][1].__setitem__("active", "yes"), priority.InvalidSnapshotError),
            (lambda s: s["streams"][1].__setitem__("deficit", 999), priority.InvalidSnapshotError),
            (lambda s: s["streams"][1].__setitem__("last_weight", -1), priority.InvalidSnapshotError),
        ],
    )
    def test_malformed_snapshot_is_rejected_before_restore(self, mutation, error):
        snapshot = readme_tree().export_state()
        mutation(snapshot)
        receiving_tree = priority.PriorityTree()
        before = receiving_tree.export_state()

        with pytest.raises(error):
            receiving_tree.restore_state(snapshot)

        assert receiving_tree.export_state() == before

    def test_snapshot_rejects_cycle_and_bad_edge_relation(self):
        cycle_snapshot = readme_tree().export_state()
        for record in cycle_snapshot["streams"]:
            if record["stream_id"] == 1:
                record["parent_id"] = 5
                record["children"] = [5]
                record["child_queue"] = [
                    entry for entry in record["child_queue"] if entry["stream_id"] == 5
                ]
            elif record["stream_id"] == 5:
                record["parent_id"] = 1
                record["children"] = [1]
                record["child_queue"] = [{"stream_id": 1, "level": 0}]
            elif record["stream_id"] == 0:
                record["children"] = [3, 7]
                record["child_queue"] = [
                    entry
                    for entry in record["child_queue"]
                    if entry["stream_id"] in (3, 7)
                ]

        with pytest.raises(priority.PriorityLoop):
            priority.PriorityTree.from_state(cycle_snapshot)

        edge_snapshot = readme_tree().export_state()
        for record in edge_snapshot["streams"]:
            if record["stream_id"] == 0:
                record["children"].remove(1)
                record["child_queue"].pop(0)
            elif record["stream_id"] == 1:
                record["parent_id"] = 0

        with pytest.raises(priority.InvalidSnapshotError):
            priority.PriorityTree.from_state(edge_snapshot)

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda s: s["streams"][0]["child_queue"].append(
                {"stream_id": 3, "level": 10}
            ),
            lambda s: s["streams"][0]["child_queue"].__setitem__(
                0,
                {"stream_id": 7, "level": 10},
            ),
            lambda s: s["streams"][0]["child_queue"].append(
                {"stream_id": 999, "level": 0}
            ),
            lambda s: s["streams"][0]["child_queue"][0].__setitem__("level", -1),
        ],
    )
    def test_snapshot_rejects_invalid_child_queue(self, mutate):
        snapshot = readme_tree().export_state()
        mutate(snapshot)

        with pytest.raises(priority.InvalidSnapshotError):
            priority.PriorityTree.from_state(snapshot)

    def test_snapshot_requires_dictionary(self):
        with pytest.raises(priority.InvalidSnapshotError):
            priority.PriorityTree.from_state(object())

    def test_snapshot_rejects_non_primitive_containers(self):
        snapshot = readme_tree().export_state()

        class SnapshotDict(dict):
            pass

        with pytest.raises(priority.InvalidSnapshotError):
            priority.PriorityTree.from_state(SnapshotDict(snapshot))

        snapshot["streams"] = tuple(snapshot["streams"])
        with pytest.raises(priority.InvalidSnapshotError):
            priority.PriorityTree.from_state(snapshot)


class TestPriorityTreeOutput:
    """
    These tests use Hypothesis to attempt to bound the output of iterating over
    the priority tree. In particular, their goal is to ensure that the output
    of the tree is "good enough": that it meets certain requirements on
    fairness and equidistribution.
    """

    @given(STREAMS_AND_WEIGHTS)
    @settings(deadline=None)
    def test_period_of_repetition(self, streams_and_weights):
        """
        The period of repetition of a priority sequence is given by the sum of
        the weights of the streams. Once that many values have been pulled out
        the sequence repeats identically.
        """
        p = priority.PriorityTree()
        weights = []

        for stream, weight in streams_and_weights:
            p.insert_stream(stream_id=stream, weight=weight)
            weights.append(weight)

        period = sum(weights)

        # Pop off the first n elements, which will always be evenly
        # distributed.
        for _ in weights:
            next(p)

        pattern = [next(p) for _ in range(period)]
        pattern = itertools.cycle(pattern)

        for i in range(period * 20):
            assert next(p) == next(pattern), i

    @given(STREAMS_AND_WEIGHTS)
    def test_priority_tree_distribution(self, streams_and_weights):
        """
        Once a full period of repetition has been observed, each stream has
        been emitted a number of times equal to its weight.
        """
        p = priority.PriorityTree()
        weights = []

        for stream, weight in streams_and_weights:
            p.insert_stream(stream_id=stream, weight=weight)
            weights.append(weight)

        period = sum(weights)

        # Pop off the first n elements, which will always be evenly
        # distributed.
        for _ in weights:
            next(p)

        count = collections.Counter(next(p) for _ in range(period))

        assert len(count) == len(streams_and_weights)
        for stream, weight in streams_and_weights:
            count[stream] == weight


class PriorityStateMachine(RuleBasedStateMachine):
    """
    This test uses Hypothesis's stateful testing to exercise the priority tree.

    It randomly inserts, removes, blocks and unblocks streams in the tree,
    then checks that iterating over priority still selects a sensible stream.
    """

    def __init__(self):
        super(PriorityStateMachine, self).__init__()
        self.tree = priority.PriorityTree()
        self.stream_ids = set([0])
        self.blocked_stream_ids = set()

    @rule(stream_id=integers())
    def insert_stream(self, stream_id):
        try:
            self.tree.insert_stream(stream_id)
        except priority.DuplicateStreamError:
            assert stream_id in self.stream_ids
        else:
            assert stream_id not in self.stream_ids
            self.stream_ids.add(stream_id)

    def _run_action(self, action, stream_id):
        try:
            action(stream_id)
        except priority.MissingStreamError:
            assert stream_id not in self.stream_ids
        except priority.PseudoStreamError:
            assert stream_id == 0
        else:
            assert stream_id in self.stream_ids

    @rule(stream_id=integers())
    def remove_stream(self, stream_id):
        self._run_action(self.tree.remove_stream, stream_id)
        if stream_id != 0:
            self.stream_ids.discard(stream_id)

    @rule(stream_id=integers())
    def block_stream(self, stream_id):
        self._run_action(self.tree.block, stream_id)
        if (stream_id != 0) and (stream_id in self.stream_ids):
            self.blocked_stream_ids.add(stream_id)

    @rule(stream_id=integers())
    def unblock_stream(self, stream_id):
        self._run_action(self.tree.unblock, stream_id)
        self.blocked_stream_ids.discard(stream_id)

    @invariant()
    def check_next_stream_consistent(self):
        """
        If we ask priority for the next stream, it always returns a sensible
        result.
        """
        try:
            next_stream_id = next(self.tree)
        except priority.DeadlockError:
            assert self.blocked_stream_ids ^ {0} == self.stream_ids
        else:
            stream = self.tree._streams[next_stream_id]

            # If a stream is selected, then it isn't blocked
            assert stream.active

            # If a stream is selected, then its parent is either the root
            # stream or blocked
            parent = stream.parent
            assert (parent.stream_id == 0) or (not parent.active)


TestPriorityTreeStateful = PriorityStateMachine.TestCase
