package main

import (
	"math/rand"
	"testing"
)

// TestIsolationForestSeparatesOutliersFromCluster is a real evaluation, not
// just a smoke test: it trains on a tight Gaussian cluster (simulating
// "normal" multivariate sensor readings) and checks that isolation forest
// assigns meaningfully higher anomaly scores to points far outside that
// cluster than to points drawn from it — the actual claim the project makes
// about this algorithm, checked against data instead of asserted.
func TestIsolationForestSeparatesOutliersFromCluster(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	// "Normal" telemetry: 3 correlated-ish features clustered near (50,50,50)
	// with small noise, like a machine running in its steady operating band.
	normal := make([][]float64, 400)
	for i := range normal {
		normal[i] = []float64{
			50 + rng.NormFloat64()*2,
			50 + rng.NormFloat64()*2,
			50 + rng.NormFloat64()*2,
		}
	}

	forest := FitIsolationForest(normal, 100, 256, rng)

	// Score a held-out batch of in-distribution points.
	normalScores := make([]float64, 50)
	for i := range normalScores {
		x := []float64{50 + rng.NormFloat64()*2, 50 + rng.NormFloat64()*2, 50 + rng.NormFloat64()*2}
		normalScores[i] = forest.Score(x)
	}

	// Score clear outliers: far outside the training cluster in every
	// dimension, the way a real spike/fault would look.
	outliers := [][]float64{
		{200, 200, 200},
		{-100, 50, 50},
		{50, 300, 50},
		{0, 0, 0},
		{500, -200, 100},
	}
	outlierScores := make([]float64, len(outliers))
	for i, x := range outliers {
		outlierScores[i] = forest.Score(x)
	}

	avgNormal := average(normalScores)
	avgOutlier := average(outlierScores)

	t.Logf("avg normal score = %.4f, avg outlier score = %.4f", avgNormal, avgOutlier)

	if avgOutlier <= avgNormal {
		t.Fatalf("expected outliers to score higher than normal points on average: normal=%.4f outlier=%.4f", avgNormal, avgOutlier)
	}
	// The isolation forest literature treats scores above ~0.6 as anomalous
	// and around/below 0.5 as normal for well-separated data; this dataset
	// is deliberately well-separated, so both bounds should hold clearly.
	if avgOutlier < 0.6 {
		t.Errorf("expected outliers to clear the conventional 0.6 anomaly threshold, got %.4f", avgOutlier)
	}
	if avgNormal > 0.6 {
		t.Errorf("expected normal points to stay below the 0.6 anomaly threshold, got %.4f", avgNormal)
	}
}

func TestIsolationForestHandlesConstantFeatureGracefully(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	// Second feature is constant across all training data — buildTree must
	// fall back gracefully instead of panicking or infinite-looping.
	data := make([][]float64, 100)
	for i := range data {
		data[i] = []float64{rng.Float64() * 10, 5.0}
	}
	forest := FitIsolationForest(data, 20, 64, rng)
	score := forest.Score([]float64{5, 5})
	if score < 0 || score > 1 {
		t.Fatalf("score out of [0,1] range: %v", score)
	}
}

func TestAveragePathLengthNormalizerKnownValues(t *testing.T) {
	if averagePathLengthNormalizer(1) != 0 {
		t.Errorf("c(1) should be 0")
	}
	if averagePathLengthNormalizer(2) != 1 {
		t.Errorf("c(2) should be 1")
	}
	if c256 := averagePathLengthNormalizer(256); c256 < 9 || c256 > 11 {
		t.Errorf("c(256) = %v, expected roughly 10 (standard isolation forest reference value)", c256)
	}
}

func average(xs []float64) float64 {
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}
