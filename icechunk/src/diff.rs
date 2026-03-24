use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use icechunk_types::Move;
use itertools::Itertools as _;

use crate::{
    format::{ChunkIndices, NodeId, Path, transaction_log::TransactionLog},
    session::{Session, SessionResult},
};

#[derive(Debug, Default)]
pub struct DiffBuilder {
    new_groups: HashSet<NodeId>,
    new_arrays: HashSet<NodeId>,
    deleted_groups: HashSet<NodeId>,
    deleted_arrays: HashSet<NodeId>,
    updated_groups: HashSet<NodeId>,
    updated_arrays: HashSet<NodeId>,
    // we use sorted set here to simply move it to a diff without having to rebuild
    updated_chunks: HashMap<NodeId, BTreeSet<ChunkIndices>>,
    moved_nodes: Vec<Move>,
}

impl DiffBuilder {
    pub fn add_changes(&mut self, tx: &TransactionLog) {
        self.new_groups.extend(tx.new_groups());
        self.new_arrays.extend(tx.new_arrays());
        self.deleted_groups.extend(tx.deleted_groups());
        self.deleted_arrays.extend(tx.deleted_arrays());
        self.updated_groups.extend(tx.updated_groups());
        self.updated_arrays.extend(tx.updated_arrays());
        self.moved_nodes.extend(tx.moves());

        for (node, chunks) in tx.updated_chunks() {
            match self.updated_chunks.get_mut(&node) {
                Some(all_chunks) => {
                    all_chunks.extend(chunks);
                }
                None => {
                    self.updated_chunks.insert(node, BTreeSet::from_iter(chunks));
                }
            }
        }
    }

    /// Returns true if the builder contains any non-chunk changes
    /// (new/deleted/updated arrays or groups, or moves).
    pub fn has_structural_changes(&self) -> bool {
        !self.new_groups.is_empty()
            || !self.new_arrays.is_empty()
            || !self.deleted_groups.is_empty()
            || !self.deleted_arrays.is_empty()
            || !self.updated_groups.is_empty()
            || !self.updated_arrays.is_empty()
            || !self.moved_nodes.is_empty()
    }

    /// Returns the set of `(NodeId, Vec<ChunkIndices>)` where both
    /// builders have modified the same chunks.
    pub fn overlapping_chunks(
        &self,
        other: &DiffBuilder,
    ) -> Vec<(NodeId, Vec<ChunkIndices>)> {
        let mut result = Vec::new();
        for (node_id, our_chunks) in &self.updated_chunks {
            if let Some(their_chunks) = other.updated_chunks.get(node_id) {
                let overlap: Vec<ChunkIndices> =
                    our_chunks.intersection(their_chunks).cloned().collect();
                if !overlap.is_empty() {
                    result.push((node_id.clone(), overlap));
                }
            }
        }
        result
    }

    /// Returns an iterator over the `NodeId`s that have chunk updates.
    pub fn updated_chunk_node_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.updated_chunks.keys()
    }

    pub async fn to_diff(self, from: &Session, to: &Session) -> SessionResult<Diff> {
        let nodes: HashMap<NodeId, Path> = from
            .list_nodes(&Path::root())
            .await?
            .chain(to.list_nodes(&Path::root()).await?)
            .map_ok(|n| (n.id, n.path))
            .try_collect()?;
        Ok(Diff::from_diff_builder(self, &nodes))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Diff {
    pub new_groups: BTreeSet<Path>,
    pub new_arrays: BTreeSet<Path>,
    pub deleted_groups: BTreeSet<Path>,
    pub deleted_arrays: BTreeSet<Path>,
    pub updated_groups: BTreeSet<Path>,
    pub updated_arrays: BTreeSet<Path>,
    pub updated_chunks: BTreeMap<Path, BTreeSet<ChunkIndices>>,
    pub moved_nodes: Vec<Move>,
}

impl Diff {
    fn from_diff_builder(builder: DiffBuilder, nodes: &HashMap<NodeId, Path>) -> Self {
        let new_groups = builder
            .new_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let new_arrays = builder
            .new_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let deleted_groups = builder
            .deleted_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let deleted_arrays = builder
            .deleted_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_groups = builder
            .updated_groups
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_arrays = builder
            .updated_arrays
            .iter()
            .flat_map(|node_id| nodes.get(node_id))
            .cloned()
            .collect();
        let updated_chunks = builder
            .updated_chunks
            .into_iter()
            .flat_map(|(node_id, chunks)| {
                let path = nodes.get(&node_id).cloned()?;
                Some((path, chunks))
            })
            .collect();
        Self {
            new_groups,
            new_arrays,
            deleted_groups,
            deleted_arrays,
            updated_groups,
            updated_arrays,
            updated_chunks,
            moved_nodes: builder.moved_nodes,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.new_groups.is_empty()
            && self.new_arrays.is_empty()
            && self.deleted_groups.is_empty()
            && self.deleted_arrays.is_empty()
            && self.updated_groups.is_empty()
            && self.updated_arrays.is_empty()
            && self.updated_chunks.is_empty()
            && self.moved_nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_builder_has_structural_changes() {
        let mut builder = DiffBuilder::default();
        assert!(!builder.has_structural_changes());

        builder.new_arrays.insert(NodeId::random());
        assert!(builder.has_structural_changes());
    }

    #[test]
    fn test_diff_builder_chunk_overlap_disjoint() {
        let node = NodeId::random();
        let mut a = DiffBuilder::default();
        let mut b = DiffBuilder::default();

        a.updated_chunks.insert(node.clone(), BTreeSet::from([ChunkIndices(vec![0, 0])]));
        b.updated_chunks.insert(node.clone(), BTreeSet::from([ChunkIndices(vec![1, 1])]));

        assert!(a.overlapping_chunks(&b).is_empty());
    }

    #[test]
    fn test_diff_builder_chunk_overlap_conflict() {
        let node = NodeId::random();
        let mut a = DiffBuilder::default();
        let mut b = DiffBuilder::default();

        a.updated_chunks.insert(node.clone(), BTreeSet::from([ChunkIndices(vec![0, 0])]));
        b.updated_chunks.insert(
            node.clone(),
            BTreeSet::from([ChunkIndices(vec![0, 0]), ChunkIndices(vec![1, 1])]),
        );

        let overlaps = a.overlapping_chunks(&b);
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].0, node);
        assert_eq!(overlaps[0].1, vec![ChunkIndices(vec![0, 0])]);
    }
}
