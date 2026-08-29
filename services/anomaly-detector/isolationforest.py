"""Isolation Forest, the Python port of isolationforest.go.

Implements the algorithm from Liu, Ting & Zhou (2008): anomalies are "few
and different," so they get isolated by random axis-aligned splits in far
fewer steps than normal points do. Each tree is built from a small random
subsample (not the full dataset), which is what makes the algorithm cheap
enough to retrain periodically on streaming data rather than needing a
one-time offline batch job.

Ported from scratch rather than swapped for scikit-learn's IsolationForest
deliberately: sklearn's score_samples()/decision_function() convention
differs from this scoring formula, and this service's ForestScoreThreshold
(0.62) plus the severity bands built on top of it are calibrated to these
exact numerics — swapping libraries would mean re-deriving that threshold
from scratch, not a drop-in replacement. See tests/test_isolationforest.py
for the same evaluation-against-real-data test as the Go version's
TestIsolationForestSeparatesOutliersFromCluster.
"""

from __future__ import annotations

import math
import random


class _ITreeNode:
    """One node of an isolation tree: either an internal split node or a
    leaf carrying the count of training points that landed there."""

    __slots__ = ("is_leaf", "size", "split_feature", "split_value", "left", "right")

    def __init__(
        self,
        is_leaf: bool = False,
        size: int = 0,
        split_feature: int = 0,
        split_value: float = 0.0,
        left: "_ITreeNode | None" = None,
        right: "_ITreeNode | None" = None,
    ):
        self.is_leaf = is_leaf
        self.size = size
        self.split_feature = split_feature
        self.split_value = split_value
        self.left = left
        self.right = right


class IsolationForest:
    def __init__(self, trees: list[_ITreeNode], subsample_size: int, avg_path_norm_c: float):
        self._trees = trees
        self._subsample_size = subsample_size
        self._avg_path_norm_c = avg_path_norm_c

    def score(self, x: list[float]) -> float:
        """Returns an anomaly score in [0,1]: values near 1 indicate the
        point was isolated unusually quickly (few splits needed) across the
        forest, which is the isolation-forest definition of "anomalous."
        Values near or below 0.5 indicate a typical point."""
        if not self._trees or self._avg_path_norm_c == 0:
            return 0.0
        total = sum(_path_length(x, tree, 0) for tree in self._trees)
        avg_path = total / len(self._trees)
        return math.pow(2, -avg_path / self._avg_path_norm_c)


def fit_isolation_forest(data: list[list[float]], num_trees: int, subsample_size: int, rng: random.Random) -> IsolationForest:
    """Trains num_trees isolation trees, each on an independent random
    subsample of size subsample_size drawn from data (or all of data, if
    data is smaller than subsample_size)."""
    if subsample_size > len(data):
        subsample_size = len(data)
    max_depth = math.ceil(math.log2(max(subsample_size, 2)))

    trees = []
    for _ in range(num_trees):
        sample = _sample_without_replacement(data, subsample_size, rng)
        trees.append(_build_tree(sample, 0, max_depth, rng))

    return IsolationForest(trees, subsample_size, average_path_length_normalizer(subsample_size))


def _build_tree(data: list[list[float]], depth: int, max_depth: int, rng: random.Random) -> _ITreeNode:
    if depth >= max_depth or len(data) <= 1:
        return _ITreeNode(is_leaf=True, size=len(data))

    num_features = len(data[0])
    # Try a few random features in case the first pick happens to be
    # constant across this subsample (no valid split possible).
    for _ in range(num_features):
        feature = rng.randrange(num_features)
        lo, hi = _feature_range(data, feature)
        if lo == hi:
            continue
        split_value = lo + rng.random() * (hi - lo)

        left = [row for row in data if row[feature] < split_value]
        right = [row for row in data if row[feature] >= split_value]
        if not left or not right:
            continue  # degenerate split, try another feature

        return _ITreeNode(
            split_feature=feature,
            split_value=split_value,
            left=_build_tree(left, depth + 1, max_depth, rng),
            right=_build_tree(right, depth + 1, max_depth, rng),
        )

    # Every feature was constant across this subsample: it can't be split
    # further, so treat it as a leaf (this is the correct, documented
    # behavior for isolation forests on degenerate data, not a workaround).
    return _ITreeNode(is_leaf=True, size=len(data))


def _path_length(x: list[float], node: _ITreeNode, depth: int) -> float:
    if node.is_leaf:
        return depth + average_path_length_normalizer(node.size)
    if x[node.split_feature] < node.split_value:
        return _path_length(x, node.left, depth + 1)
    return _path_length(x, node.right, depth + 1)


def average_path_length_normalizer(n: int) -> float:
    """c(n): the expected path length of an unsuccessful search in a
    binary search tree of n nodes, used both to account for leaves holding
    more than one point and to normalize the overall forest score into
    [0,1]."""
    if n <= 1:
        return 0.0
    if n == 2:
        return 1.0
    euler_mascheroni = 0.5772156649
    harmonic = math.log(n - 1) + euler_mascheroni
    return 2 * harmonic - (2 * (n - 1) / n)


def _feature_range(data: list[list[float]], feature: int) -> tuple[float, float]:
    lo = hi = data[0][feature]
    for row in data:
        if row[feature] < lo:
            lo = row[feature]
        if row[feature] > hi:
            hi = row[feature]
    return lo, hi


def _sample_without_replacement(data: list[list[float]], n: int, rng: random.Random) -> list[list[float]]:
    if n >= len(data):
        return list(data)
    return rng.sample(data, n)
