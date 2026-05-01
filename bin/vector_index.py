#!/usr/bin/env python3
"""Vector Index — FAISS HNSW wrapper for Chronicle memory.

Provides fast approximate nearest neighbor search over capsule, pattern,
and observation embeddings. Replaces the pure-Python linear scan in memory.py.

Usage:
    from vector_index import VectorIndex
    idx = VectorIndex("/mnt/hdd/chronicle-data/capsules.faiss")
    idx.add([1, 2, 3], [vec1, vec2, vec3])
    results = idx.search(query_vec, k=20)  # [(id, similarity), ...]
"""

import json
import os
import sys
import time
import numpy as np
from typing import List, Tuple, Optional

import faiss

# Default paths
DATA_DIR = os.environ.get("CHRONICLE_DATA_DIR", "/mnt/hdd/chronicle-data")
DEFAULT_DIM = 768  # nomic-embed-text dimension (Build #125)

# HNSW parameters — tuned for 20K-500K vectors
HNSW_M = 32              # connections per node (higher = better recall, more RAM)
HNSW_EF_CONSTRUCTION = 200  # build-time quality (higher = better graph, slower build)
HNSW_EF_SEARCH = 64       # query-time quality (higher = better recall, slower search)


def _normalize(vec: np.ndarray) -> np.ndarray:
    """L2-normalize a vector for cosine similarity via inner product."""
    norm = np.linalg.norm(vec)
    if norm == 0:
        return vec
    return vec / norm


class VectorIndex:
    """FAISS HNSW index with ID mapping and cosine similarity.

    FAISS uses sequential internal IDs. We maintain a bidirectional mapping
    between external IDs (capsule_id, pattern_id, etc.) and FAISS internal IDs.

    Vectors are L2-normalized before insertion so that inner product = cosine similarity.
    """

    def __init__(self, index_path: str, dim: int = DEFAULT_DIM):
        self.index_path = index_path
        self.id_map_path = index_path.replace(".faiss", "_ids.json")
        self.dim = dim
        self.index: Optional[faiss.Index] = None
        self._ext_to_int: dict = {}  # external_id -> faiss_internal_id
        self._int_to_ext: dict = {}  # faiss_internal_id -> external_id
        self._next_internal = 0
        self._load()

    def _load(self):
        """Load existing index from disk, or create new one."""
        if os.path.exists(self.index_path) and os.path.exists(self.id_map_path):
            try:
                self.index = faiss.read_index(self.index_path)
                with open(self.id_map_path) as f:
                    mapping = json.load(f)
                self._ext_to_int = {int(k): v for k, v in mapping.get("ext_to_int", {}).items()}
                self._int_to_ext = {int(k): v for k, v in mapping.get("int_to_ext", {}).items()}
                self._next_internal = mapping.get("next_internal", self.index.ntotal)
                # Set search parameters
                self.index.hnsw.efSearch = HNSW_EF_SEARCH
            except Exception as e:
                print(f"[vector_index] Load error, creating new: {e}")
                self._create_new()
        else:
            self._create_new()

    def _create_new(self):
        """Create a fresh HNSW index."""
        # Inner product on normalized vectors = cosine similarity
        self.index = faiss.IndexHNSWFlat(self.dim, HNSW_M)
        self.index.hnsw.efConstruction = HNSW_EF_CONSTRUCTION
        self.index.hnsw.efSearch = HNSW_EF_SEARCH
        self._ext_to_int = {}
        self._int_to_ext = {}
        self._next_internal = 0

    def add(self, ids: List[int], vectors: List[List[float]]):
        """Add vectors with external IDs. Skips duplicates."""
        if not ids or not vectors:
            return

        new_ids = []
        new_vecs = []

        for ext_id, vec in zip(ids, vectors):
            if ext_id in self._ext_to_int:
                continue  # already indexed
            if len(vec) != self.dim:
                continue  # wrong dimension

            internal_id = self._next_internal
            self._ext_to_int[ext_id] = internal_id
            self._int_to_ext[internal_id] = ext_id
            self._next_internal += 1

            new_ids.append(internal_id)
            # Normalize for cosine similarity
            new_vecs.append(_normalize(np.array(vec, dtype=np.float32)))

        if new_vecs:
            batch = np.vstack(new_vecs)
            self.index.add(batch)

    def search(self, query_vec: List[float], k: int = 20,
               threshold: float = 0.0) -> List[Tuple[int, float]]:
        """Search for nearest neighbors.

        Returns list of (external_id, distance) sorted by distance (ascending).
        For L2 distance: lower = more similar.
        """
        if self.index.ntotal == 0:
            return []

        q = _normalize(np.array([query_vec], dtype=np.float32))
        actual_k = min(k, self.index.ntotal)
        D, I = self.index.search(q, actual_k)

        results = []
        for dist, internal_id in zip(D[0], I[0]):
            if internal_id < 0:
                continue  # FAISS returns -1 for missing results
            ext_id = self._int_to_ext.get(int(internal_id))
            if ext_id is None:
                continue
            # Convert L2 distance to cosine similarity approximation
            # For normalized vectors: cos_sim = 1 - (L2_dist^2 / 2)
            cos_sim = 1.0 - (float(dist) / 2.0)
            if cos_sim >= threshold:
                results.append((ext_id, cos_sim))

        return results

    def search_ids(self, query_vec: List[float], candidate_ids: List[int],
                   k: int = 20) -> List[Tuple[int, float]]:
        """Search only within a set of candidate IDs (for cluster-filtered search).

        Falls back to full search if candidates aren't indexed.
        """
        if not candidate_ids or self.index.ntotal == 0:
            return []

        # Get internal IDs for candidates
        internal_ids = []
        for ext_id in candidate_ids:
            iid = self._ext_to_int.get(ext_id)
            if iid is not None:
                internal_ids.append(iid)

        if not internal_ids:
            return []

        # Use IDSelectorArray for filtered search
        sel = faiss.IDSelectorArray(len(internal_ids),
                                     faiss.swig_ptr(np.array(internal_ids, dtype=np.int64)))
        params = faiss.SearchParametersHNSW()
        params.sel = sel

        q = _normalize(np.array([query_vec], dtype=np.float32))
        actual_k = min(k, len(internal_ids))
        D, I = self.index.search(q, actual_k, params=params)

        results = []
        for dist, internal_id in zip(D[0], I[0]):
            if internal_id < 0:
                continue
            ext_id = self._int_to_ext.get(int(internal_id))
            if ext_id is None:
                continue
            cos_sim = 1.0 - (float(dist) / 2.0)
            results.append((ext_id, cos_sim))

        return results

    def contains(self, ext_id: int) -> bool:
        """Check if an external ID is in the index."""
        return ext_id in self._ext_to_int

    def count(self) -> int:
        """Return number of vectors in the index."""
        return self.index.ntotal

    def save(self):
        """Persist index and ID mapping to disk."""
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
        faiss.write_index(self.index, self.index_path)
        mapping = {
            "ext_to_int": self._ext_to_int,
            "int_to_ext": self._int_to_ext,
            "next_internal": self._next_internal,
        }
        with open(self.id_map_path, "w") as f:
            json.dump(mapping, f)

    def stats(self) -> dict:
        """Return index statistics."""
        mem_bytes = self.index.ntotal * self.dim * 4  # float32
        return {
            "total_vectors": self.index.ntotal,
            "dimension": self.dim,
            "memory_mb": round(mem_bytes / (1024 * 1024), 1),
            "index_path": self.index_path,
            "hnsw_m": HNSW_M,
            "hnsw_ef_search": HNSW_EF_SEARCH,
        }


# ═══════════════════════════════════════════════════════════════════
#  CLI & Testing
# ═══════════════════════════════════════════════════════════════════

def _test():
    """Smoke test: create, add, search, verify."""
    import tempfile

    print("Testing VectorIndex...")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.faiss")
        idx = VectorIndex(path, dim=1024)

        # Generate test vectors
        np.random.seed(42)
        n = 10000
        vecs = np.random.random((n, 1024)).astype("float32").tolist()
        ids = list(range(1, n + 1))

        # Add
        print(f"  Adding {n} vectors...")
        t0 = time.time()
        idx.add(ids, vecs)
        add_ms = (time.time() - t0) * 1000
        print(f"  Added in {add_ms:.0f}ms")
        assert idx.count() == n, f"Expected {n}, got {idx.count()}"

        # Search
        query = vecs[0]  # search for first vector
        t0 = time.time()
        results = idx.search(query, k=10)
        search_ms = (time.time() - t0) * 1000
        print(f"  Search: {search_ms:.1f}ms, {len(results)} results")
        assert len(results) > 0, "No results"
        # First result should be the query itself (ID=1)
        assert results[0][0] == 1, f"Expected ID 1, got {results[0][0]}"
        print(f"  Top result: ID={results[0][0]}, similarity={results[0][1]:.4f}")

        # Duplicate add (should skip)
        idx.add([1, 2], [vecs[0], vecs[1]])
        assert idx.count() == n, f"Duplicate add changed count: {idx.count()}"

        # Save and reload
        idx.save()
        idx2 = VectorIndex(path, dim=1024)
        assert idx2.count() == n, f"Reload count mismatch: {idx2.count()}"
        results2 = idx2.search(query, k=10)
        assert results2[0][0] == results[0][0], "Reload search mismatch"
        print(f"  Save/reload verified")

        # Scale test
        print(f"\n  Scale test: {n} vectors")
        times = []
        for _ in range(100):
            q = np.random.random(1024).astype("float32").tolist()
            t0 = time.time()
            idx.search(q, k=20)
            times.append((time.time() - t0) * 1000)
        avg = sum(times) / len(times)
        p99 = sorted(times)[98]
        print(f"  100 searches: avg={avg:.2f}ms, p99={p99:.2f}ms")

        # Stats
        s = idx.stats()
        print(f"\n  Stats: {s}")

    print("\nAll tests passed.")


def _benchmark(n: int = 100000):
    """Benchmark at scale."""
    import tempfile

    print(f"Benchmarking VectorIndex at {n} vectors...")
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "bench.faiss")
        idx = VectorIndex(path, dim=1024)

        vecs = np.random.random((n, 1024)).astype("float32").tolist()
        ids = list(range(1, n + 1))

        t0 = time.time()
        # Add in batches of 1000
        for i in range(0, n, 1000):
            idx.add(ids[i:i+1000], vecs[i:i+1000])
        build_s = time.time() - t0
        print(f"  Build: {build_s:.1f}s for {n} vectors")

        times = []
        for _ in range(100):
            q = np.random.random(1024).astype("float32").tolist()
            t0 = time.time()
            idx.search(q, k=20)
            times.append((time.time() - t0) * 1000)
        avg = sum(times) / len(times)
        p99 = sorted(times)[98]
        print(f"  Search (100 queries): avg={avg:.2f}ms, p99={p99:.2f}ms")

        s = idx.stats()
        print(f"  Memory: {s['memory_mb']} MB")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] == "test":
        _test()
    elif args[0] == "bench":
        n = int(args[1]) if len(args) > 1 else 100000
        _benchmark(n)
    else:
        print("Usage: vector_index.py [test|bench [N]]")
