# Branch Merge v2: Structural Changes — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:executing-plans to implement this plan task-by-task.

**Goal:** Extend `Repository::merge_branches()` to support non-conflicting structural changes (new/deleted/updated arrays and groups) using snapshot-based three-way comparison.

**Architecture:** Replace the blanket `has_structural_changes()` rejection gate with per-node conflict detection using the three already-fetched snapshots (ancestor, source tip, target tip). Keep `DiffBuilder::overlapping_chunks()` for chunk-level conflict detection. Moves remain rejected.

**Tech Stack:** Rust (icechunk crate), PyO3 bindings, pytest, cargo-nextest

**Design doc:** `docs/plans/2026-03-24-branch-merge-structural-design.md`

---

### Task 1: Add DiffBuilder accessor methods

**Files:**
- Modify: `icechunk/src/diff.rs:22-80`
- Test: `icechunk/src/diff.rs:176-217` (existing test module)

**Step 1: Write failing tests for the new accessors**

Add to the existing `mod tests` in `icechunk/src/diff.rs`:

```rust
#[test]
fn test_diff_builder_has_chunk_updates_for() {
    let node = NodeId::random();
    let other = NodeId::random();
    let mut builder = DiffBuilder::default();

    assert!(!builder.has_chunk_updates_for(&node));

    builder.updated_chunks.insert(
        node.clone(),
        BTreeSet::from([ChunkIndices(vec![0])]),
    );

    assert!(builder.has_chunk_updates_for(&node));
    assert!(!builder.has_chunk_updates_for(&other));
}

#[test]
fn test_diff_builder_has_moves() {
    let mut builder = DiffBuilder::default();
    assert!(!builder.has_moves());

    builder.moved_nodes.push(Move {
        from: "/a".try_into().unwrap(),
        to: "/b".try_into().unwrap(),
    });
    assert!(builder.has_moves());
}
```

**Step 2: Run tests to verify they fail**

Run: `just test -- -E 'test(has_chunk_updates_for) | test(has_moves)'`
Expected: FAIL — methods don't exist

**Step 3: Implement the accessors**

Add after `updated_chunk_node_ids()` (line 80) in `icechunk/src/diff.rs`:

```rust
/// Returns true if the given node has any chunk updates.
pub fn has_chunk_updates_for(&self, node_id: &NodeId) -> bool {
    self.updated_chunks.contains_key(node_id)
}

/// Returns true if the builder contains any move operations.
pub fn has_moves(&self) -> bool {
    !self.moved_nodes.is_empty()
}
```

**Step 4: Run tests to verify they pass**

Run: `just test -- -E 'test(has_chunk_updates_for) | test(has_moves)'`
Expected: PASS

**Step 5: Commit**

```bash
git add icechunk/src/diff.rs
git commit -m "feat(merge): add DiffBuilder accessor methods for merge conflict detection"
```

---

### Task 2: Add MergeConflictKind error type

**Files:**
- Modify: `icechunk/src/repository.rs:96-169` (RepositoryErrorKind enum)

**Step 1: Add the new error variant**

In `icechunk/src/repository.rs`, add `MergeConflictKind` enum before `RepositoryErrorKind` (around line 95), and add a new variant to `RepositoryErrorKind`:

Add before the `RepositoryErrorKind` enum:

```rust
/// Describes why a branch merge could not be completed.
#[derive(Debug, Clone)]
pub enum MergeConflictKind {
    /// Both branches modified the same chunk coordinates.
    ChunkOverlap {
        node_id: NodeId,
        chunks: Vec<ChunkIndices>,
    },
    /// Both branches modified metadata on the same node.
    BothModifiedMetadata { path: Path },
    /// One branch deleted a node the other modified.
    DeleteModifyConflict { path: Path },
    /// Both branches created a node at the same path.
    BothCreatedAtPath { path: Path },
}

impl std::fmt::Display for MergeConflictKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChunkOverlap { node_id, chunks } => {
                write!(
                    f,
                    "overlapping chunk modifications on node \
                     {node_id:?}: {chunks:?}"
                )
            }
            Self::BothModifiedMetadata { path } => {
                write!(
                    f,
                    "both branches modified metadata at {path}"
                )
            }
            Self::DeleteModifyConflict { path } => {
                write!(
                    f,
                    "one branch deleted and the other modified \
                     node at {path}"
                )
            }
            Self::BothCreatedAtPath { path } => {
                write!(
                    f,
                    "both branches created a node at {path}"
                )
            }
        }
    }
}
```

Replace the existing `MergeConflict` variant (line 165-166):

```rust
// Old:
// #[error("merge conflict: overlapping chunk modifications")]
// MergeConflict { conflicts: Vec<(NodeId, Vec<ChunkIndices>)> },

// New:
#[error("merge conflict: {}", conflicts.iter().map(|c| c.to_string()).collect::<Vec<_>>().join("; "))]
MergeConflict { conflicts: Vec<MergeConflictKind> },
```

**Step 2: Fix compile errors from the old MergeConflict usage**

Update the existing chunk overlap error in `merge_branches()` (around line 2139) to use the new type:

```rust
// Old:
// return Err(RepositoryError::capture(RepositoryErrorKind::MergeConflict {
//     conflicts: overlaps,
// }));

// New:
let conflicts = overlaps
    .into_iter()
    .map(|(node_id, chunks)| MergeConflictKind::ChunkOverlap {
        node_id,
        chunks,
    })
    .collect();
return Err(RepositoryError::capture(
    RepositoryErrorKind::MergeConflict { conflicts },
));
```

Update the test assertion in `test_merge_branches_overlapping_chunks_error` (around line 4918-4921) — the match pattern stays the same (`RepositoryErrorKind::MergeConflict { .. }`).

**Step 3: Verify it compiles and existing tests pass**

Run: `just build && just test -- -E 'test(merge)'`
Expected: All existing merge tests pass

**Step 4: Commit**

```bash
git add icechunk/src/repository.rs
git commit -m "feat(merge): add MergeConflictKind enum for richer merge error reporting"
```

---

### Task 3: Add node_metadata_eq helper function

**Files:**
- Modify: `icechunk/src/repository.rs` (add helper near `build_merge_snapshot`, around line 2220)

**Step 1: Write the failing test**

Add a test in the merge test section of `icechunk/src/repository.rs` (after line 5157):

```rust
#[test]
fn test_node_metadata_eq() {
    use icechunk_format::snapshot::{
        ArrayShape, NodeData, NodeSnapshot,
    };
    use icechunk_format::manifest::ManifestRef;

    let path: Path = "/test".try_into().unwrap();
    let id = NodeId::random();

    let group_a = NodeSnapshot {
        id: id.clone(),
        path: path.clone(),
        user_data: Bytes::from_static(b"data"),
        node_data: NodeData::Group,
    };
    let group_b = NodeSnapshot {
        id: id.clone(),
        path: path.clone(),
        user_data: Bytes::from_static(b"data"),
        node_data: NodeData::Group,
    };
    // Same groups are equal
    assert!(node_metadata_eq(&group_a, &group_b));

    // Different user_data
    let group_c = NodeSnapshot {
        id: id.clone(),
        path: path.clone(),
        user_data: Bytes::from_static(b"other"),
        node_data: NodeData::Group,
    };
    assert!(!node_metadata_eq(&group_a, &group_c));

    // Arrays with same metadata but different manifests
    let array_a = NodeSnapshot {
        id: id.clone(),
        path: path.clone(),
        user_data: Bytes::new(),
        node_data: NodeData::Array {
            shape: ArrayShape::new(vec![(10, 1)]).unwrap(),
            dimension_names: None,
            manifests: vec![],
        },
    };
    let array_b = NodeSnapshot {
        id: id.clone(),
        path: path.clone(),
        user_data: Bytes::new(),
        node_data: NodeData::Array {
            shape: ArrayShape::new(vec![(10, 1)]).unwrap(),
            dimension_names: None,
            manifests: vec![ManifestRef {
                object_id: ManifestId::random(),
                extents: Default::default(),
            }],
        },
    };
    // Manifests differ but metadata is equal
    assert!(node_metadata_eq(&array_a, &array_b));

    // Different shape
    let array_c = NodeSnapshot {
        id: id.clone(),
        path: path.clone(),
        user_data: Bytes::new(),
        node_data: NodeData::Array {
            shape: ArrayShape::new(vec![(20, 2)]).unwrap(),
            dimension_names: None,
            manifests: vec![],
        },
    };
    assert!(!node_metadata_eq(&array_a, &array_c));

    // Group vs array
    assert!(!node_metadata_eq(&group_a, &array_a));
}
```

**Step 2: Run test to verify it fails**

Run: `just test -- -E 'test(node_metadata_eq)'`
Expected: FAIL — function doesn't exist

**Step 3: Implement the helper**

Add before `build_merge_snapshot` in `icechunk/src/repository.rs` (around line 2220):

```rust
/// Compare two node snapshots ignoring manifest references.
///
/// For arrays, compares shape, dimension_names, and user_data
/// but NOT manifests (which naturally diverge between branches).
/// For groups, compares user_data only.
fn node_metadata_eq(a: &NodeSnapshot, b: &NodeSnapshot) -> bool {
    if a.user_data != b.user_data {
        return false;
    }
    match (&a.node_data, &b.node_data) {
        (
            NodeData::Array {
                shape: s1,
                dimension_names: d1,
                ..
            },
            NodeData::Array {
                shape: s2,
                dimension_names: d2,
                ..
            },
        ) => s1 == s2 && d1 == d2,
        (NodeData::Group, NodeData::Group) => true,
        _ => false,
    }
}
```

**Step 4: Run test to verify it passes**

Run: `just test -- -E 'test(node_metadata_eq)'`
Expected: PASS

**Step 5: Commit**

```bash
git add icechunk/src/repository.rs
git commit -m "feat(merge): add node_metadata_eq helper for snapshot comparison"
```

---

### Task 4: Rewrite merge gate and build_merge_snapshot

This is the core task. It replaces the `has_structural_changes()` gate with moves-only gating, and rewrites `build_merge_snapshot` to use the 8-case matrix.

**Files:**
- Modify: `icechunk/src/repository.rs:2112-2134` (merge gate)
- Modify: `icechunk/src/repository.rs:2225-2352` (build_merge_snapshot)

**Step 1: Replace the merge gate**

In `merge_branches()`, replace lines 2112-2134:

```rust
// Old: blanket structural changes rejection
// if source_changes.has_structural_changes() { ... }
// if target_changes.has_structural_changes() { ... }

// New: only reject moves
if source_changes.has_moves() {
    return Err(RepositoryError::capture(
        RepositoryErrorKind::MergeNotSupported {
            reason: format!(
                "source branch '{source}' has move operations \
                 which cannot be merged"
            ),
        },
    ));
}
if target_changes.has_moves() {
    return Err(RepositoryError::capture(
        RepositoryErrorKind::MergeNotSupported {
            reason: format!(
                "target branch '{target}' has move operations \
                 which cannot be merged"
            ),
        },
    ));
}
```

**Step 2: Rewrite build_merge_snapshot**

Replace the body of `build_merge_snapshot` (lines 2225-2352) with the unified path iteration approach. The new implementation:

1. Fetches 3 snapshots (same as before)
2. Builds a `BTreeMap<Path, (Option<NodeSnapshot>, Option<NodeSnapshot>, Option<NodeSnapshot>)>` from all three snapshots
3. Iterates the map, applying the 8-case matrix
4. Collects conflicts into `Vec<MergeConflictKind>`
5. If any conflicts, returns error. Otherwise builds the snapshot.

```rust
async fn build_merge_snapshot(
    &self,
    ancestor_id: &SnapshotId,
    source_branch: &str,
    target_branch: &str,
    source_changes: &DiffBuilder,
    target_changes: &DiffBuilder,
    message: &str,
) -> RepositoryResult<(Arc<Snapshot>, TransactionLog)> {
    let source_tip_id =
        self.lookup_branch(source_branch).await?;
    let target_tip_id =
        self.lookup_branch(target_branch).await?;

    let (ancestor_snap, source_snap, target_snap) = try_join!(
        self.asset_manager.fetch_snapshot(ancestor_id),
        self.asset_manager.fetch_snapshot(&source_tip_id),
        self.asset_manager.fetch_snapshot(&target_tip_id),
    )?;

    // Build unified path map: path -> (ancestor, source, target)
    let mut path_map: BTreeMap<
        Path,
        (
            Option<NodeSnapshot>,
            Option<NodeSnapshot>,
            Option<NodeSnapshot>,
        ),
    > = BTreeMap::new();

    for node_result in ancestor_snap.iter() {
        let node = node_result.inject()?;
        path_map
            .entry(node.path.clone())
            .or_insert((None, None, None))
            .0 = Some(node);
    }
    for node_result in source_snap.iter() {
        let node = node_result.inject()?;
        path_map
            .entry(node.path.clone())
            .or_insert((None, None, None))
            .1 = Some(node);
    }
    for node_result in target_snap.iter() {
        let node = node_result.inject()?;
        path_map
            .entry(node.path.clone())
            .or_insert((None, None, None))
            .2 = Some(node);
    }

    // Collect manifest files from all three snapshots
    let mut manifest_files: HashSet<ManifestFileInfo> =
        HashSet::new();
    for mf in source_snap.manifest_files() {
        manifest_files.insert(mf);
    }
    for mf in target_snap.manifest_files() {
        manifest_files.insert(mf);
    }
    for mf in ancestor_snap.manifest_files() {
        manifest_files.insert(mf);
    }

    let mut all_nodes: Vec<NodeSnapshot> = Vec::new();
    let mut conflicts: Vec<MergeConflictKind> = Vec::new();

    for (path, (ancestor, source, target)) in &path_map {
        match (ancestor, source, target) {
            // Case 1: exists in all three
            (Some(anc), Some(src), Some(tgt)) => {
                let src_modified =
                    !node_metadata_eq(anc, src);
                let tgt_modified =
                    !node_metadata_eq(anc, tgt);

                if src_modified && tgt_modified {
                    // Both modified metadata -> conflict
                    conflicts.push(
                        MergeConflictKind::BothModifiedMetadata {
                            path: path.clone(),
                        },
                    );
                    continue;
                }

                // Check chunk overlaps for this node
                let chunk_overlaps =
                    source_changes.overlapping_chunks(
                        target_changes,
                    );
                let node_chunk_conflict = chunk_overlaps
                    .iter()
                    .any(|(nid, _)| *nid == anc.id);
                if node_chunk_conflict {
                    let chunks = chunk_overlaps
                        .into_iter()
                        .filter(|(nid, _)| *nid == anc.id)
                        .flat_map(|(_, c)| c)
                        .collect();
                    conflicts.push(
                        MergeConflictKind::ChunkOverlap {
                            node_id: anc.id.clone(),
                            chunks,
                        },
                    );
                    continue;
                }

                let src_chunks = source_changes
                    .has_chunk_updates_for(&anc.id);
                let tgt_chunks = target_changes
                    .has_chunk_updates_for(&anc.id);

                if src_chunks && tgt_chunks {
                    // Both wrote chunks, disjoint — combine
                    // manifests from both tips
                    if let (
                        NodeData::Array {
                            manifests: s_manifests,
                            ..
                        },
                        NodeData::Array {
                            manifests: t_manifests,
                            ..
                        },
                    ) = (&src.node_data, &tgt.node_data)
                    {
                        let mut merged_manifests = Vec::new();
                        merged_manifests
                            .extend(s_manifests.iter().cloned());
                        merged_manifests
                            .extend(t_manifests.iter().cloned());

                        // Take metadata from whichever side
                        // modified it, or ancestor if neither
                        let base = if src_modified {
                            src
                        } else if tgt_modified {
                            tgt
                        } else {
                            anc
                        };
                        all_nodes.push(NodeSnapshot {
                            id: base.id.clone(),
                            path: base.path.clone(),
                            user_data: base.user_data.clone(),
                            node_data: NodeData::Array {
                                manifests: merged_manifests,
                                shape: match &base.node_data {
                                    NodeData::Array {
                                        shape, ..
                                    } => shape.clone(),
                                    _ => unreachable!(),
                                },
                                dimension_names: match &base
                                    .node_data
                                {
                                    NodeData::Array {
                                        dimension_names,
                                        ..
                                    } => dimension_names.clone(),
                                    _ => unreachable!(),
                                },
                            },
                        });
                    } else {
                        all_nodes.push(anc.clone());
                    }
                } else if src_modified || src_chunks {
                    // Only source changed -> take source
                    all_nodes.push(src.clone());
                } else if tgt_modified || tgt_chunks {
                    // Only target changed -> take target
                    all_nodes.push(tgt.clone());
                } else {
                    // Neither changed -> keep ancestor
                    all_nodes.push(anc.clone());
                }
            }

            // Case 2: target deleted (absent from target)
            (Some(anc), Some(src), None) => {
                let src_modified =
                    !node_metadata_eq(anc, src);
                let src_chunks = source_changes
                    .has_chunk_updates_for(&anc.id);
                if src_modified || src_chunks {
                    conflicts.push(
                        MergeConflictKind::DeleteModifyConflict {
                            path: path.clone(),
                        },
                    );
                }
                // else: safe delete, omit node
            }

            // Case 3: source deleted (absent from source)
            (Some(anc), None, Some(tgt)) => {
                let tgt_modified =
                    !node_metadata_eq(anc, tgt);
                let tgt_chunks = target_changes
                    .has_chunk_updates_for(&anc.id);
                if tgt_modified || tgt_chunks {
                    conflicts.push(
                        MergeConflictKind::DeleteModifyConflict {
                            path: path.clone(),
                        },
                    );
                }
                // else: safe delete, omit node
            }

            // Case 4: both deleted -> omit
            (Some(_), None, None) => {}

            // Case 5: source created
            (None, Some(src), None) => {
                all_nodes.push(src.clone());
            }

            // Case 6: target created
            (None, None, Some(tgt)) => {
                all_nodes.push(tgt.clone());
            }

            // Case 7: both created at same path
            (None, Some(_), Some(_)) => {
                conflicts.push(
                    MergeConflictKind::BothCreatedAtPath {
                        path: path.clone(),
                    },
                );
            }

            // Case 8: doesn't exist anywhere
            (None, None, None) => {}
        }
    }

    if !conflicts.is_empty() {
        return Err(RepositoryError::capture(
            RepositoryErrorKind::MergeConflict { conflicts },
        ));
    }

    // Sort by path for from_iter
    all_nodes.sort_by(|a, b| a.path.cmp(&b.path));

    let parent_id =
        if self.spec_version == SpecVersionBin::V1 {
            Some(target_tip_id.clone())
        } else {
            None
        };

    let new_snapshot = Snapshot::from_iter(
        None,
        parent_id,
        self.spec_version,
        message,
        None,
        manifest_files.into_iter().collect(),
        None,
        all_nodes
            .into_iter()
            .map(Ok::<_, IcechunkFormatError>),
    )
    .inject()?;

    // Build merged transaction log
    let new_snapshot_id = new_snapshot.id();
    let source_logs = self
        .collect_transaction_logs(source_branch, ancestor_id)
        .await?;
    let target_logs = self
        .collect_transaction_logs(target_branch, ancestor_id)
        .await?;
    let all_logs: Vec<&TransactionLog> = source_logs
        .iter()
        .chain(target_logs.iter())
        .map(|l| l.as_ref())
        .collect();
    let merged_tx_log =
        TransactionLog::merge(&new_snapshot_id, all_logs);

    Ok((Arc::new(new_snapshot), merged_tx_log))
}
```

**Step 3: Remove the separate chunk overlap check from merge_branches**

The chunk overlap check (lines 2136-2142 in `merge_branches`) is now handled inside `build_merge_snapshot` per-node. Remove it:

```rust
// Delete these lines from merge_branches:
// let overlaps = source_changes.overlapping_chunks(&target_changes);
// if !overlaps.is_empty() {
//     return Err(RepositoryError::capture(RepositoryErrorKind::MergeConflict {
//         conflicts: overlaps,
//     }));
// }
```

**Step 4: Verify existing tests still pass**

Run: `just test -- -E 'test(merge)'`
Expected: All existing merge tests pass. Note: `test_merge_branches_structural_changes_error` will now PASS the merge instead of failing — this test needs updating in Task 5.

**Step 5: Commit**

```bash
git add icechunk/src/repository.rs
git commit -m "feat(merge): rewrite merge to support structural changes via snapshot comparison"
```

---

### Task 5: Update existing test for structural changes

The existing `test_merge_branches_structural_changes_error` (lines 4927-4977) tests that both branches creating arrays causes rejection. With our changes, this should now succeed (disjoint paths). Update it.

**Files:**
- Modify: `icechunk/src/repository.rs:4927-4977`

**Step 1: Update the test to expect success**

Replace the test body to verify the merge succeeds and both arrays are present:

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_branches_structural_changes_disjoint(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    // Setup: initial commit with root group
    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;

    repo.create_branch("feature", &snap0).await?;

    // Add an array on main
    let mut session = repo.writable_session("main").await?;
    session
        .add_array(
            "/main_array".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session.commit("main: new array").execute().await?;

    // Add a different array on feature
    let mut session = repo.writable_session("feature").await?;
    session
        .add_array(
            "/feature_array".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session.commit("feature: new array").execute().await?;

    // Merge should succeed now
    let merge_snap = repo
        .merge_branches(
            "feature",
            "main",
            "merge structural",
        )
        .await?;

    // Verify both arrays exist
    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(merge_snap))
        .await?;
    let main_arr =
        session.get_array(&"/main_array".try_into()?).await;
    let feature_arr =
        session.get_array(&"/feature_array".try_into()?).await;
    assert!(main_arr.is_ok());
    assert!(feature_arr.is_ok());

    Ok(())
}
```

**Step 2: Run the updated test**

Run: `just test -- -E 'test(structural_changes)'`
Expected: PASS

**Step 3: Commit**

```bash
git add icechunk/src/repository.rs
git commit -m "test(merge): update structural changes test to expect success for disjoint paths"
```

---

### Task 6: Add Rust tests for new merge cases

**Files:**
- Modify: `icechunk/src/repository.rs` (test module, after line 5157)

Add tests one at a time. Each subtask is: write test, run it, verify it passes.

**Step 1: Test — source adds new array, target writes chunks**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_source_adds_array_target_writes_chunks(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    session
        .add_array(
            "/existing".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Source adds a new array
    let mut session = repo.writable_session("feature").await?;
    session
        .add_array(
            "/new_array".try_into()?,
            ArrayShape::new(vec![(5, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session.commit("feature: new array").execute().await?;

    // Target writes chunks to existing array
    let mut session = repo.writable_session("main").await?;
    session
        .set_chunk_ref(
            "/existing".try_into()?,
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline(
                Bytes::from_static(b"data"),
            )),
        )
        .await?;
    session.commit("main: write chunk").execute().await?;

    let merge_snap = repo
        .merge_branches("feature", "main", "merge")
        .await?;

    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(merge_snap))
        .await?;
    assert!(
        session
            .get_array(&"/new_array".try_into()?)
            .await
            .is_ok()
    );
    assert!(
        session
            .get_chunk_ref(
                &"/existing".try_into()?,
                &ChunkIndices(vec![0])
            )
            .await?
            .is_some()
    );
    Ok(())
}
```

Run: `just test -- -E 'test(source_adds_array_target_writes)'`
Expected: PASS

**Step 2: Test — both create at same path (conflict)**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_both_create_same_path_conflict(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Both create array at /new_array
    let mut session = repo.writable_session("main").await?;
    session
        .add_array(
            "/new_array".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session.commit("main: new array").execute().await?;

    let mut session = repo.writable_session("feature").await?;
    session
        .add_array(
            "/new_array".try_into()?,
            ArrayShape::new(vec![(5, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session.commit("feature: new array").execute().await?;

    let result = repo
        .merge_branches("feature", "main", "conflict")
        .await;
    assert!(matches!(
        result.unwrap_err().kind,
        RepositoryErrorKind::MergeConflict { .. },
    ));
    Ok(())
}
```

Run: `just test -- -E 'test(both_create_same_path)'`
Expected: PASS

**Step 3: Test — source deletes, target untouched**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_source_deletes_untouched_array(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    session
        .add_array(
            "/to_delete".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session
        .add_array(
            "/keeper".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Source deletes an array
    let mut session = repo.writable_session("feature").await?;
    session
        .delete_array("/to_delete".try_into()?)
        .await?;
    session.commit("feature: delete").execute().await?;

    // Target does nothing (or writes to a different array)
    let mut session = repo.writable_session("main").await?;
    session
        .set_chunk_ref(
            "/keeper".try_into()?,
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline(
                Bytes::from_static(b"data"),
            )),
        )
        .await?;
    session.commit("main: write keeper").execute().await?;

    let merge_snap = repo
        .merge_branches("feature", "main", "merge delete")
        .await?;

    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(merge_snap))
        .await?;
    // Deleted array should be gone
    assert!(
        session
            .get_array(&"/to_delete".try_into()?)
            .await
            .is_err()
    );
    // Keeper should have its chunk
    assert!(
        session
            .get_chunk_ref(
                &"/keeper".try_into()?,
                &ChunkIndices(vec![0])
            )
            .await?
            .is_some()
    );
    Ok(())
}
```

Run: `just test -- -E 'test(source_deletes_untouched)'`
Expected: PASS

**Step 4: Test — delete-modify conflict**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_delete_modify_conflict(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    session
        .add_array(
            "/array".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Source deletes the array
    let mut session = repo.writable_session("feature").await?;
    session
        .delete_array("/array".try_into()?)
        .await?;
    session.commit("feature: delete").execute().await?;

    // Target writes chunks to it
    let mut session = repo.writable_session("main").await?;
    session
        .set_chunk_ref(
            "/array".try_into()?,
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline(
                Bytes::from_static(b"data"),
            )),
        )
        .await?;
    session.commit("main: write chunk").execute().await?;

    let result = repo
        .merge_branches("feature", "main", "conflict")
        .await;
    assert!(matches!(
        result.unwrap_err().kind,
        RepositoryErrorKind::MergeConflict { .. },
    ));
    Ok(())
}
```

Run: `just test -- -E 'test(delete_modify_conflict)'`
Expected: PASS

**Step 5: Test — both modify metadata conflict**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_both_modify_metadata_conflict(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    session
        .add_array(
            "/array".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Both branches update metadata
    let mut session = repo.writable_session("main").await?;
    session
        .update_array(
            &"/array".try_into()?,
            ArrayShape::new(vec![(20, 2)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session.commit("main: update shape").execute().await?;

    let mut session = repo.writable_session("feature").await?;
    session
        .update_array(
            &"/array".try_into()?,
            ArrayShape::new(vec![(30, 3)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session
        .commit("feature: update shape")
        .execute()
        .await?;

    let result = repo
        .merge_branches("feature", "main", "conflict")
        .await;
    assert!(matches!(
        result.unwrap_err().kind,
        RepositoryErrorKind::MergeConflict { .. },
    ));
    Ok(())
}
```

Run: `just test -- -E 'test(both_modify_metadata)'`
Expected: PASS

**Step 6: Test — one modifies metadata, other writes chunks (success)**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_metadata_update_and_chunk_write(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    session
        .add_array(
            "/array".try_into()?,
            ArrayShape::new(vec![(10, 2)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Source updates metadata
    let mut session = repo.writable_session("feature").await?;
    session
        .update_array(
            &"/array".try_into()?,
            ArrayShape::new(vec![(20, 2)]).unwrap(),
            None,
            Bytes::from_static(b"updated"),
        )
        .await?;
    session
        .commit("feature: update metadata")
        .execute()
        .await?;

    // Target writes chunks
    let mut session = repo.writable_session("main").await?;
    session
        .set_chunk_ref(
            "/array".try_into()?,
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline(
                Bytes::from_static(b"data"),
            )),
        )
        .await?;
    session.commit("main: write chunk").execute().await?;

    let merge_snap = repo
        .merge_branches("feature", "main", "merge")
        .await?;

    // Verify metadata from source and chunk from target
    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(merge_snap))
        .await?;
    let node =
        session.get_array(&"/array".try_into()?).await?;
    assert_eq!(
        node.user_data,
        Bytes::from_static(b"updated")
    );
    assert!(
        session
            .get_chunk_ref(
                &"/array".try_into()?,
                &ChunkIndices(vec![0])
            )
            .await?
            .is_some()
    );
    Ok(())
}
```

Run: `just test -- -E 'test(metadata_update_and_chunk)'`
Expected: PASS

**Step 7: Test — both delete same node (success)**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_both_delete_same_node(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    session
        .add_array(
            "/array".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Both delete it
    let mut session = repo.writable_session("main").await?;
    session
        .delete_array("/array".try_into()?)
        .await?;
    session.commit("main: delete").execute().await?;

    let mut session = repo.writable_session("feature").await?;
    session
        .delete_array("/array".try_into()?)
        .await?;
    session.commit("feature: delete").execute().await?;

    let merge_snap = repo
        .merge_branches("feature", "main", "merge deletes")
        .await?;

    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(merge_snap))
        .await?;
    assert!(
        session
            .get_array(&"/array".try_into()?)
            .await
            .is_err()
    );
    Ok(())
}
```

Run: `just test -- -E 'test(both_delete_same)'`
Expected: PASS

**Step 8: Test — combined structural + chunk changes**

```rust
#[tokio_test]
#[apply(spec_version_cases)]
async fn test_merge_combined_structural_and_chunks(
    spec_version: SpecVersionBin,
) -> Result<(), Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> =
        new_in_memory_storage().await?;
    let repo = Repository::create(
        None,
        Arc::clone(&storage),
        HashMap::new(),
        Some(spec_version),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    session
        .add_group(
            Path::root(),
            Bytes::copy_from_slice(b""),
        )
        .await?;
    session
        .add_array(
            "/existing".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session
        .add_array(
            "/to_delete".try_into()?,
            ArrayShape::new(vec![(10, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    let snap0 = session.commit("initial").execute().await?;
    repo.create_branch("feature", &snap0).await?;

    // Source: add new array + delete another
    let mut session = repo.writable_session("feature").await?;
    session
        .add_array(
            "/brand_new".try_into()?,
            ArrayShape::new(vec![(5, 1)]).unwrap(),
            None,
            Bytes::new(),
        )
        .await?;
    session
        .delete_array("/to_delete".try_into()?)
        .await?;
    session
        .commit("feature: add + delete")
        .execute()
        .await?;

    // Target: write chunks to existing
    let mut session = repo.writable_session("main").await?;
    session
        .set_chunk_ref(
            "/existing".try_into()?,
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline(
                Bytes::from_static(b"data"),
            )),
        )
        .await?;
    session.commit("main: write chunk").execute().await?;

    let merge_snap = repo
        .merge_branches("feature", "main", "merge combined")
        .await?;

    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(merge_snap))
        .await?;
    // New array from source present
    assert!(
        session
            .get_array(&"/brand_new".try_into()?)
            .await
            .is_ok()
    );
    // Deleted array gone
    assert!(
        session
            .get_array(&"/to_delete".try_into()?)
            .await
            .is_err()
    );
    // Chunk from target present
    assert!(
        session
            .get_chunk_ref(
                &"/existing".try_into()?,
                &ChunkIndices(vec![0])
            )
            .await?
            .is_some()
    );
    Ok(())
}
```

Run: `just test -- -E 'test(combined_structural_and_chunks)'`
Expected: PASS

**Step 9: Run all merge tests together**

Run: `just test -- -E 'test(merge)'`
Expected: ALL PASS

**Step 10: Commit**

```bash
git add icechunk/src/repository.rs
git commit -m "test(merge): add comprehensive tests for structural merge cases"
```

---

### Task 7: Add Python integration tests

**Files:**
- Modify: `icechunk-python/tests/test_merge.py`

**Step 1: Add test — create variable on one branch, write data on other**

```python
def test_merge_new_variable_and_data(
    repo: icechunk.Repository,
) -> None:
    """Merge succeeds when one branch adds a variable and the
    other writes data to an existing one."""
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store)
    root.create_array("existing", shape=(10,), dtype="f8")
    snap0 = session.commit("initial")

    repo.create_branch("feature", snap0)

    # Feature adds a new variable
    session = repo.writable_session("feature")
    store = session.store
    root = zarr.open_group(store=store, mode="r+")
    root.create_array("new_var", shape=(5,), dtype="i4")
    session.commit("feature: add new_var")

    # Main writes data to existing
    session = repo.writable_session("main")
    store = session.store
    arr = zarr.open_array(
        store=store, path="existing", mode="r+"
    )
    arr[0] = 42.0
    session.commit("main: write existing")

    merge_snap = repo.merge_branches(
        "feature", "main", "merge"
    )

    session = repo.readonly_session(snapshot_id=merge_snap)
    store = session.store
    arr = zarr.open_array(
        store=store, path="existing", mode="r"
    )
    new_var = zarr.open_array(
        store=store, path="new_var", mode="r"
    )
    assert arr[0] == 42.0
    assert new_var.shape == (5,)
```

**Step 2: Add test — delete variable on one branch**

```python
def test_merge_delete_variable(
    repo: icechunk.Repository,
) -> None:
    """Merge succeeds when one branch deletes an untouched
    variable."""
    session = repo.writable_session("main")
    store = session.store
    root = zarr.group(store=store)
    root.create_array("keep", shape=(10,), dtype="f8")
    root.create_array("remove", shape=(10,), dtype="f8")
    snap0 = session.commit("initial")

    repo.create_branch("feature", snap0)

    # Feature deletes 'remove'
    session = repo.writable_session("feature")
    store = session.store
    del zarr.open_group(store=store, mode="r+")["remove"]
    session.commit("feature: delete remove")

    # Main writes to 'keep'
    session = repo.writable_session("main")
    store = session.store
    arr = zarr.open_array(
        store=store, path="keep", mode="r+"
    )
    arr[0] = 1.0
    session.commit("main: write keep")

    merge_snap = repo.merge_branches(
        "feature", "main", "merge delete"
    )

    session = repo.readonly_session(snapshot_id=merge_snap)
    store = session.store
    arr = zarr.open_array(
        store=store, path="keep", mode="r"
    )
    assert arr[0] == 1.0
    with pytest.raises(Exception):
        zarr.open_array(
            store=store, path="remove", mode="r"
        )
```

**Step 3: Add test — both create same variable name (conflict)**

```python
def test_merge_both_create_same_variable_conflict(
    repo: icechunk.Repository,
) -> None:
    """Merge fails when both branches create a variable at the
    same path."""
    session = repo.writable_session("main")
    store = session.store
    zarr.group(store=store)
    snap0 = session.commit("initial")

    repo.create_branch("feature", snap0)

    session = repo.writable_session("main")
    store = session.store
    root = zarr.open_group(store=store, mode="r+")
    root.create_array("conflict", shape=(10,), dtype="f8")
    session.commit("main: create conflict")

    session = repo.writable_session("feature")
    store = session.store
    root = zarr.open_group(store=store, mode="r+")
    root.create_array("conflict", shape=(5,), dtype="i4")
    session.commit("feature: create conflict")

    with pytest.raises(Exception):
        repo.merge_branches(
            "feature", "main", "conflict merge"
        )
```

**Step 4: Run Python tests**

Run: `just pytest -- -k test_merge -v`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add icechunk-python/tests/test_merge.py
git commit -m "test(python): add integration tests for structural merge"
```

---

### Task 8: Run full test suite and lint

**Step 1: Run Rust lints**

Run: `just lint`
Expected: No warnings or errors

**Step 2: Run full Rust tests**

Run: `just test`
Expected: ALL PASS

**Step 3: Run Python tests**

Run: `just pytest`
Expected: ALL PASS

**Step 4: Run format check**

Run: `just format --check && just ruff`
Expected: No formatting issues

**Step 5: Fix any issues found, commit if needed**

```bash
git add -u
git commit -m "fix: address lint/format issues from merge structural changes"
```
