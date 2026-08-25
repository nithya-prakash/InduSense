package main

import (
	"math"
	"math/rand"
)

// iTreeNode is one node of an isolation tree: either an internal split node
// or a leaf carrying the count of training points that landed there.
type iTreeNode struct {
	isLeaf       bool
	size         int
	splitFeature int
	splitValue   float64
	left, right  *iTreeNode
}

// IsolationForest implements the algorithm from Liu, Ting & Zhou (2008):
// anomalies are "few and different," so they get isolated by random
// axis-aligned splits in far fewer steps than normal points do. Each tree is
// built from a small random subsample (not the full dataset), which is what
// makes the algorithm cheap enough to retrain periodically on streaming
// data rather than needing a one-time offline batch job.
type IsolationForest struct {
	trees         []*iTreeNode
	subsampleSize int
	avgPathNormC  float64 // c(subsampleSize): expected path length of an unsuccessful BST search, used to normalize scores into [0,1]
}

// FitIsolationForest trains numTrees isolation trees, each on an independent
// random subsample of size subsampleSize drawn from data (or all of data, if
// data is smaller than subsampleSize).
func FitIsolationForest(data [][]float64, numTrees, subsampleSize int, rng *rand.Rand) *IsolationForest {
	if subsampleSize > len(data) {
		subsampleSize = len(data)
	}
	maxDepth := int(math.Ceil(math.Log2(math.Max(float64(subsampleSize), 2))))

	trees := make([]*iTreeNode, 0, numTrees)
	for i := 0; i < numTrees; i++ {
		sample := sampleWithoutReplacement(data, subsampleSize, rng)
		trees = append(trees, buildTree(sample, 0, maxDepth, rng))
	}

	return &IsolationForest{
		trees:         trees,
		subsampleSize: subsampleSize,
		avgPathNormC:  averagePathLengthNormalizer(subsampleSize),
	}
}

// Score returns an anomaly score in [0,1]: values near 1 indicate the point
// was isolated unusually quickly (few splits needed) across the forest,
// which is the isolation-forest definition of "anomalous." Values near or
// below 0.5 indicate a typical point.
func (f *IsolationForest) Score(x []float64) float64 {
	if len(f.trees) == 0 || f.avgPathNormC == 0 {
		return 0
	}
	total := 0.0
	for _, tree := range f.trees {
		total += pathLength(x, tree, 0)
	}
	avgPath := total / float64(len(f.trees))
	return math.Pow(2, -avgPath/f.avgPathNormC)
}

func buildTree(data [][]float64, depth, maxDepth int, rng *rand.Rand) *iTreeNode {
	if depth >= maxDepth || len(data) <= 1 {
		return &iTreeNode{isLeaf: true, size: len(data)}
	}

	numFeatures := len(data[0])
	// Try a few random features in case the first pick happens to be
	// constant across this subsample (no valid split possible).
	for attempt := 0; attempt < numFeatures; attempt++ {
		feature := rng.Intn(numFeatures)
		min, max := featureRange(data, feature)
		if min == max {
			continue
		}
		splitValue := min + rng.Float64()*(max-min)

		var left, right [][]float64
		for _, row := range data {
			if row[feature] < splitValue {
				left = append(left, row)
			} else {
				right = append(right, row)
			}
		}
		if len(left) == 0 || len(right) == 0 {
			continue // degenerate split, try another feature
		}

		return &iTreeNode{
			splitFeature: feature,
			splitValue:   splitValue,
			left:         buildTree(left, depth+1, maxDepth, rng),
			right:        buildTree(right, depth+1, maxDepth, rng),
		}
	}

	// Every feature was constant across this subsample: it can't be split
	// further, so treat it as a leaf (this is the correct, documented
	// behavior for isolation forests on degenerate data, not a workaround).
	return &iTreeNode{isLeaf: true, size: len(data)}
}

func pathLength(x []float64, node *iTreeNode, depth int) float64 {
	if node.isLeaf {
		return float64(depth) + averagePathLengthNormalizer(node.size)
	}
	if x[node.splitFeature] < node.splitValue {
		return pathLength(x, node.left, depth+1)
	}
	return pathLength(x, node.right, depth+1)
}

// averagePathLengthNormalizer is c(n): the expected path length of an
// unsuccessful search in a binary search tree of n nodes, used both to
// account for leaves holding more than one point and to normalize the
// overall forest score into [0,1].
func averagePathLengthNormalizer(n int) float64 {
	if n <= 1 {
		return 0
	}
	if n == 2 {
		return 1
	}
	const eulerMascheroni = 0.5772156649
	harmonic := math.Log(float64(n-1)) + eulerMascheroni
	return 2*harmonic - (2 * float64(n-1) / float64(n))
}

func featureRange(data [][]float64, feature int) (min, max float64) {
	min, max = data[0][feature], data[0][feature]
	for _, row := range data {
		if row[feature] < min {
			min = row[feature]
		}
		if row[feature] > max {
			max = row[feature]
		}
	}
	return min, max
}

func sampleWithoutReplacement(data [][]float64, n int, rng *rand.Rand) [][]float64 {
	if n >= len(data) {
		out := make([][]float64, len(data))
		copy(out, data)
		return out
	}
	perm := rng.Perm(len(data))[:n]
	out := make([][]float64, n)
	for i, idx := range perm {
		out[i] = data[idx]
	}
	return out
}
