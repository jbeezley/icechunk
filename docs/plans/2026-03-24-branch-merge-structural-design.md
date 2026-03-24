# Branch Merge v2: Structural Changes

**Date:** 2026-03-24
**Status:** Approved
**Extends:** `2026-03-24-branch-merge-design.md` (v1, chunks-only)

## Goal

Extend `Repository::merge_branches()` to support non-conflicting structural changes (new/deleted/updated arrays and groups) in addition to the existing disjoint chunk merge. Moves remain unsupported. Safety is the top priority: if we cannot prove a merge is safe, reject it.

## Design Principles

1. **No merge can silently lose data.** Every node in the merged snapshot is either identical to the ancestor, taken from the only branch that modified it, a chunk-level combination of disjoint writes, or intentionally omitted because it was safely deleted.
2. **Minimal blast radius.** Changes are confined to `build_merge_snapshot` and the merge gate in `merge_branches`. No format changes, no changes to transaction logs or change sets.
3. **Reject rather than guess.** Ambiguous cases are conflicts, not heuristics.

## Approach: Snapshot-Based Three-Way Comparison

Replace the blanket `has_structural_changes()` rejection with a per-node classification using the three snapshots (ancestor, source tip, target tip) that are already fetched.

### Node Metadata Equality

For comparing whether a branch modified a node (excluding chunk data):

- **Arrays:** Compare `shape`, `dimension_names`, and `user_data`. Exclude `manifests` (these naturally diverge between branches).
- **Groups:** Compare `user_data` only.
- **Type change** (group became array or vice versa): treat as not equal.

```rust
fn node_metadata_eq(a: &NodeSnapshot, b: &NodeSnapshot) -> bool {
    if a.user_data != b.user_data { return false; }
    match (&a.node_data, &b.node_data) {
        (NodeData::Array { shape: s1, dimension_names: d1, .. },
         NodeData::Array { shape: s2, dimension_names: d2, .. }) => s1 == s2 && d1 == d2,
        (NodeData::Group, NodeData::Group) => true,
        _ => false,
    }
}
```

### The 8-Case Matrix

For each path across all three snapshots:

| # | Ancestor | Source | Target | Action |
|---|----------|--------|--------|--------|
| 1 | yes | yes | yes | Check metadata. Both modified -> **conflict**. One modified -> take that version. Neither -> keep ancestor. Chunk overlap check for arrays. |
| 2 | yes | yes | no | Target deleted. If source didn't modify metadata AND source has no chunk updates for this node -> safe delete (omit). Otherwise -> **conflict**. |
| 3 | yes | no | yes | Source deleted. Mirror of case 2. |
| 4 | yes | no | no | Both deleted -> safe, omit. |
| 5 | no | yes | no | Source created -> include from source. |
| 6 | no | no | yes | Target created -> include from target. |
| 7 | no | yes | yes | Both created at same path -> **conflict**. |
| 8 | no | no | no | N/A. |

### Moves Gate

If either branch's `DiffBuilder` contains moves, reject with `MergeNotSupported` as today. This is the only remaining blanket gate.

## Changes to `build_merge_snapshot`

### Current flow (what changes)

1. Fetch 3 snapshots -- **no change**
2. Build `source_arrays` / `target_arrays` from chunk-modified NodeIds -- **replaced**
3. Iterate only ancestor nodes -- **replaced with unified path iteration**
4. Per-node chunk-only logic -- **replaced with 8-case matrix**

### New flow

1. Fetch 3 snapshots (unchanged)
2. Build unified path map from all three snapshots: `BTreeMap<Path, (Option<NodeSnapshot>, Option<NodeSnapshot>, Option<NodeSnapshot>)>`
3. For each path, apply the 8-case matrix
4. Use `DiffBuilder::overlapping_chunks()` for case 1 chunk conflict detection
5. Use `DiffBuilder::has_chunk_updates_for()` for cases 2-3 delete-modify detection
6. Collect manifest files from all three snapshots (unchanged)
7. Build snapshot from merged node list (unchanged)

## Changes to `DiffBuilder`

- **Add** `has_chunk_updates_for(&NodeId) -> bool` -- O(1) lookup for delete-modify detection
- **Add** `has_moves() -> bool` -- replaces `has_structural_changes()` as the merge gate
- **Keep** `overlapping_chunks()` and `updated_chunk_node_ids()` unchanged
- **Keep** `has_structural_changes()` method (useful utility, just no longer the merge gate)

## Error Types

Extend `RepositoryErrorKind` with richer merge conflict information:

```rust
pub enum MergeConflictKind {
    ChunkOverlap { node_id: NodeId, chunks: Vec<ChunkIndices> },
    BothModifiedMetadata { path: Path },
    DeleteModifyConflict { path: Path },
    BothCreatedAtPath { path: Path },
}
```

Replace or extend the existing `MergeConflict` variant to carry `Vec<MergeConflictKind>`.

## Scope Boundaries

### In scope

- New arrays/groups created on one or both branches (disjoint paths)
- Deleted arrays/groups (when the other branch didn't touch them)
- Updated array metadata (shape, dimension_names, user_data) on one branch
- Updated group metadata (user_data) on one branch
- All existing chunk merge behavior
- Combinations of the above

### Out of scope (rejected with error)

- Moves/renames on either branch
- Both branches modify metadata on the same node
- One branch deletes a node the other modified (metadata or chunks)
- Both branches create a node at the same path
- Overlapping chunk writes on the same array

## Testing Strategy

### Rust unit tests (13 cases)

1. Source adds new array, target only writes chunks -> success
2. Target adds new group, source only writes chunks -> success
3. Both add new arrays at different paths -> success
4. Both add nodes at the same path -> `BothCreatedAtPath`
5. Source deletes array, target doesn't touch it -> success
6. Source deletes array, target wrote chunks to it -> `DeleteModifyConflict`
7. Source deletes array, target updated its metadata -> `DeleteModifyConflict`
8. Both delete the same array -> success
9. Source updates array metadata, target doesn't touch it -> success
10. Both update metadata on the same array -> `BothModifiedMetadata`
11. Source updates metadata, target writes chunks to same array -> success
12. Moves on either branch -> `MergeNotSupported`
13. Combined: source adds array + deletes another, target writes chunks -> success

### Python integration tests (3-4 cases)

- Create a variable on one branch, write data on the other, merge, verify both present
- Delete a variable on one branch, merge, verify it's gone
- Conflict: both branches create same variable name

## Estimated Code Changes

| File | Change | Lines (est.) |
|------|--------|-------------|
| `icechunk/src/repository.rs` | Replace gate, rewrite `build_merge_snapshot`, extend error types | +95, -60 |
| `icechunk/src/repository.rs` | 13 new test cases | +400 |
| `icechunk/src/diff.rs` | `has_chunk_updates_for()` and `has_moves()` accessors | +7 |
| `icechunk-python/tests/test_merge.py` | 3-4 integration tests | +80 |
| `icechunk-python/src/repository.rs` | Update error mapping | ~5 |

Net: ~150 lines of new logic, ~400 lines of tests. No format changes.
