import random

from isolationforest import average_path_length_normalizer, fit_isolation_forest


def test_isolation_forest_separates_outliers_from_cluster():
    """A real evaluation, not just a smoke test: trains on a tight Gaussian
    cluster (simulating "normal" multivariate sensor readings) and checks
    that isolation forest assigns meaningfully higher anomaly scores to
    points far outside that cluster than to points drawn from it — the
    actual claim the project makes about this algorithm, checked against
    data instead of asserted."""
    rng = random.Random(42)

    # "Normal" telemetry: 3 correlated-ish features clustered near (50,50,50)
    # with small noise, like a machine running in its steady operating band.
    normal = [[50 + rng.gauss(0, 2), 50 + rng.gauss(0, 2), 50 + rng.gauss(0, 2)] for _ in range(400)]

    forest = fit_isolation_forest(normal, 100, 256, rng)

    # Score a held-out batch of in-distribution points.
    normal_scores = [
        forest.score([50 + rng.gauss(0, 2), 50 + rng.gauss(0, 2), 50 + rng.gauss(0, 2)]) for _ in range(50)
    ]

    # Score clear outliers: far outside the training cluster in every
    # dimension, the way a real spike/fault would look.
    outliers = [
        [200, 200, 200],
        [-100, 50, 50],
        [50, 300, 50],
        [0, 0, 0],
        [500, -200, 100],
    ]
    outlier_scores = [forest.score(x) for x in outliers]

    avg_normal = sum(normal_scores) / len(normal_scores)
    avg_outlier = sum(outlier_scores) / len(outlier_scores)

    assert avg_outlier > avg_normal, f"expected outliers to score higher than normal points on average: normal={avg_normal:.4f} outlier={avg_outlier:.4f}"
    # The isolation forest literature treats scores above ~0.6 as anomalous
    # and around/below 0.5 as normal for well-separated data; this dataset
    # is deliberately well-separated, so both bounds should hold clearly.
    assert avg_outlier >= 0.6, f"expected outliers to clear the conventional 0.6 anomaly threshold, got {avg_outlier:.4f}"
    assert avg_normal <= 0.6, f"expected normal points to stay below the 0.6 anomaly threshold, got {avg_normal:.4f}"


def test_isolation_forest_handles_constant_feature_gracefully():
    rng = random.Random(1)
    # Second feature is constant across all training data — _build_tree must
    # fall back gracefully instead of crashing or infinite-looping.
    data = [[rng.random() * 10, 5.0] for _ in range(100)]
    forest = fit_isolation_forest(data, 20, 64, rng)
    score = forest.score([5, 5])
    assert 0 <= score <= 1


def test_average_path_length_normalizer_known_values():
    assert average_path_length_normalizer(1) == 0
    assert average_path_length_normalizer(2) == 1
    c256 = average_path_length_normalizer(256)
    assert 9 < c256 < 11, "c(256) should be roughly 10 (standard isolation forest reference value)"
