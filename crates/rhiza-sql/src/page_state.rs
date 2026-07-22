use rhiza_core::LogHash;
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

const MIN_SQLITE_PAGE_SIZE: u32 = 512;
const MAX_SQLITE_PAGE_SIZE: u32 = 65_536;
const LEAF_DOMAIN: &[u8] = b"rhiza:qwal-v3:page-state:leaf\0";
const EMPTY_DOMAIN: &[u8] = b"rhiza:qwal-v3:page-state:empty\0";
const INTERNAL_DOMAIN: &[u8] = b"rhiza:qwal-v3:page-state:internal\0";
const STATE_DOMAIN: &[u8] = b"rhiza:qwal-v3:page-state:root\0";

/// The content identity of a closed SQLite database in QWAL v3.
///
/// `state_root` binds the page size, page count, and canonical Merkle root.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StateIdentityV3 {
    pub page_size: u32,
    pub page_count: u32,
    pub state_root: LogHash,
}

/// A borrowed final page image used to calculate or install a state change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageStatePatchV3<'a> {
    page_no: u32,
    after_image: &'a [u8],
}

impl<'a> PageStatePatchV3<'a> {
    pub(crate) const fn new(page_no: u32, after_image: &'a [u8]) -> Self {
        Self {
            page_no,
            after_image,
        }
    }
}

/// Dense, rebuildable Merkle state for closed SQLite page images.
///
/// This cache is not authoritative. Callers must rebuild it from the closed
/// database whenever its provenance or consistency is uncertain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageStateCacheV3 {
    page_size: u32,
    page_count: u32,
    // `levels[0]` contains leaves. Each later level contains the parents of
    // the preceding level, with canonical empty subtrees filling the right
    // edge up to the next power of two.
    levels: Vec<Vec<LogHash>>,
}

#[derive(Clone, Copy)]
struct HashedPatch {
    page_index: u64,
    hash: LogHash,
}

impl PageStateCacheV3 {
    /// Rebuilds the cache from every page in canonical one-based order.
    pub(crate) fn from_pages<I, P>(page_size: u32, pages: I) -> Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        validate_page_size(page_size)?;

        let mut leaves = Vec::new();
        for page in pages {
            let page = page.as_ref();
            if page.len() != page_size as usize {
                return invalid("page image length does not match page size");
            }
            let page_no = u32::try_from(leaves.len() + 1)
                .map_err(|_| Error::ResourceExhausted("page count exceeds u32".into()))?;
            leaves.push(hash_leaf(page_no, page));
        }
        let page_count = u32::try_from(leaves.len())
            .map_err(|_| Error::ResourceExhausted("page count exceeds u32".into()))?;
        validate_page_count(page_count)?;

        let empty = empty_subtrees(tree_height(page_count));
        let mut levels = vec![leaves];
        for height in 1..=tree_height(page_count) as usize {
            let children = &levels[height - 1];
            let mut parents = Vec::with_capacity(children.len().div_ceil(2));
            for pair in children.chunks(2) {
                let right = pair.get(1).copied().unwrap_or(empty[height - 1]);
                parents.push(hash_internal(height as u32, pair[0], right));
            }
            levels.push(parents);
        }

        Ok(Self {
            page_size,
            page_count,
            levels,
        })
    }

    pub(crate) fn identity(&self) -> StateIdentityV3 {
        identity(self.page_size, self.page_count, self.tree_root())
    }

    /// Calculates the target identity without mutating the dense cache.
    pub(crate) fn overlay(
        &self,
        target_page_count: u32,
        patches: &[PageStatePatchV3<'_>],
    ) -> Result<StateIdentityV3> {
        let hashed = self.validate_and_hash(target_page_count, patches)?;
        Ok(self.overlay_hashed(target_page_count, &hashed))
    }

    /// Atomically validates and installs a page patch into the cache.
    ///
    /// Validation and target-root calculation finish before the cache is
    /// changed, so every returned error leaves the cache untouched.
    pub(crate) fn apply_patch(
        &mut self,
        target_page_count: u32,
        patches: &[PageStatePatchV3<'_>],
    ) -> Result<StateIdentityV3> {
        let hashed = self.validate_and_hash(target_page_count, patches)?;
        let target = self.overlay_hashed(target_page_count, &hashed);
        self.apply_hashed(target_page_count, &hashed);
        debug_assert_eq!(self.identity(), target);
        Ok(target)
    }

    fn tree_root(&self) -> LogHash {
        self.levels
            .last()
            .and_then(|level| level.first())
            .copied()
            .expect("validated page-state caches are non-empty")
    }

    fn validate_and_hash(
        &self,
        target_page_count: u32,
        patches: &[PageStatePatchV3<'_>],
    ) -> Result<Vec<HashedPatch>> {
        validate_page_count(target_page_count)?;
        usize::try_from(target_page_count)
            .map_err(|_| Error::ResourceExhausted("page count exceeds usize".into()))?;

        let mut previous = 0;
        let mut hashed = Vec::with_capacity(patches.len());
        for patch in patches {
            if patch.page_no == 0 {
                return invalid("page numbers must be one-based");
            }
            if patch.page_no <= previous {
                return invalid("page patches must be strictly ordered without duplicates");
            }
            if patch.page_no > target_page_count {
                return invalid("page patch lies outside the target state");
            }
            if patch.after_image.len() != self.page_size as usize {
                return invalid("page image length does not match page size");
            }
            hashed.push(HashedPatch {
                page_index: u64::from(patch.page_no - 1),
                hash: hash_leaf(patch.page_no, patch.after_image),
            });
            previous = patch.page_no;
        }

        if target_page_count > self.page_count {
            let first_new = patches.partition_point(|patch| patch.page_no <= self.page_count);
            let required = u64::from(target_page_count - self.page_count);
            let supplied = u64::try_from(patches.len() - first_new)
                .map_err(|_| Error::ResourceExhausted("page patch count exceeds u64".into()))?;
            if supplied != required {
                return invalid("growth must include every newly allocated page");
            }
            for (offset, patch) in patches[first_new..].iter().enumerate() {
                let offset = u32::try_from(offset)
                    .map_err(|_| Error::ResourceExhausted("page patch count exceeds u32".into()))?;
                if patch.page_no != self.page_count + 1 + offset {
                    return invalid("growth must include a complete new page suffix");
                }
            }
        }

        Ok(hashed)
    }

    fn overlay_hashed(&self, target_page_count: u32, patches: &[HashedPatch]) -> StateIdentityV3 {
        let height = tree_height(target_page_count);
        let empty = empty_subtrees(height);
        let tree_root = self.overlay_node(0, height, u64::from(target_page_count), patches, &empty);
        identity(self.page_size, target_page_count, tree_root)
    }

    fn overlay_node(
        &self,
        start: u64,
        height: u32,
        target_page_count: u64,
        patches: &[HashedPatch],
        empty: &[LogHash],
    ) -> LogHash {
        if start >= target_page_count {
            return empty[height as usize];
        }

        let width = 1_u64 << height;
        let end = start + width;
        if patches.is_empty() && end <= u64::from(self.page_count) && end <= target_page_count {
            return self.levels[height as usize][(start >> height) as usize];
        }

        if height == 0 {
            if let Some(patch) = patches.first() {
                debug_assert_eq!(patch.page_index, start);
                return patch.hash;
            }
            return self.levels[0][start as usize];
        }

        let midpoint = start + (width >> 1);
        let split = patches.partition_point(|patch| patch.page_index < midpoint);
        let left = self.overlay_node(
            start,
            height - 1,
            target_page_count,
            &patches[..split],
            empty,
        );
        let right = self.overlay_node(
            midpoint,
            height - 1,
            target_page_count,
            &patches[split..],
            empty,
        );
        hash_internal(height, left, right)
    }

    fn apply_hashed(&mut self, target_page_count: u32, patches: &[HashedPatch]) {
        let target_len = target_page_count as usize;
        let old_page_count = self.page_count;
        let height = tree_height(target_page_count) as usize;
        let empty = empty_subtrees(height as u32);

        self.levels[0].resize(target_len, empty[0]);
        self.levels[0].truncate(target_len);
        for patch in patches {
            self.levels[0][patch.page_index as usize] = patch.hash;
        }

        let mut dirty: Vec<usize> = patches
            .iter()
            .map(|patch| patch.page_index as usize)
            .collect();
        if target_page_count < old_page_count {
            dirty.push(target_len - 1);
            dirty.sort_unstable();
            dirty.dedup();
        }

        self.levels.resize_with(height + 1, Vec::new);
        for current_height in 1..=height {
            let desired_len = target_len.div_ceil(1_usize << current_height);
            self.levels[current_height].resize(desired_len, empty[current_height]);
            self.levels[current_height].truncate(desired_len);

            let mut parents: Vec<usize> = dirty.iter().map(|index| index / 2).collect();
            parents.dedup();
            for parent in &parents {
                let left_index = parent * 2;
                let (left, right) = {
                    let children = &self.levels[current_height - 1];
                    (
                        children[left_index],
                        children
                            .get(left_index + 1)
                            .copied()
                            .unwrap_or(empty[current_height - 1]),
                    )
                };
                self.levels[current_height][*parent] =
                    hash_internal(current_height as u32, left, right);
            }
            dirty = parents;
        }
        self.levels.truncate(height + 1);
        self.page_count = target_page_count;
    }
}

fn validate_page_size(page_size: u32) -> Result<()> {
    if !(MIN_SQLITE_PAGE_SIZE..=MAX_SQLITE_PAGE_SIZE).contains(&page_size)
        || !page_size.is_power_of_two()
    {
        return invalid("page size must be a power of two from 512 through 65536");
    }
    Ok(())
}

fn validate_page_count(page_count: u32) -> Result<()> {
    if page_count == 0 {
        return invalid("page count must be positive");
    }
    Ok(())
}

fn tree_height(page_count: u32) -> u32 {
    u32::BITS - (page_count - 1).leading_zeros()
}

fn hash_leaf(page_no: u32, page: &[u8]) -> LogHash {
    let page_no = page_no.to_be_bytes();
    LogHash::digest(&[LEAF_DOMAIN, &page_no, page])
}

fn hash_internal(height: u32, left: LogHash, right: LogHash) -> LogHash {
    let height = height.to_be_bytes();
    LogHash::digest(&[INTERNAL_DOMAIN, &height, left.as_bytes(), right.as_bytes()])
}

fn empty_subtrees(height: u32) -> Vec<LogHash> {
    let mut empty = Vec::with_capacity(height as usize + 1);
    empty.push(LogHash::digest(&[EMPTY_DOMAIN]));
    for current_height in 1..=height {
        let child = empty[current_height as usize - 1];
        empty.push(hash_internal(current_height, child, child));
    }
    empty
}

fn identity(page_size: u32, page_count: u32, tree_root: LogHash) -> StateIdentityV3 {
    let page_size_bytes = page_size.to_be_bytes();
    let page_count_bytes = page_count.to_be_bytes();
    StateIdentityV3 {
        page_size,
        page_count,
        state_root: LogHash::digest(&[
            STATE_DOMAIN,
            &page_size_bytes,
            &page_count_bytes,
            tree_root.as_bytes(),
        ]),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidEntry(format!(
        "invalid QWAL v3 page state: {}",
        message.into()
    )))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const PAGE_SIZE: usize = 512;

    fn page(byte: u8) -> Vec<u8> {
        vec![byte; PAGE_SIZE]
    }

    fn cache(pages: &[Vec<u8>]) -> PageStateCacheV3 {
        PageStateCacheV3::from_pages(PAGE_SIZE as u32, pages).unwrap()
    }

    fn patches(pages: &[(u32, Vec<u8>)]) -> Vec<PageStatePatchV3<'_>> {
        pages
            .iter()
            .map(|(page_no, image)| PageStatePatchV3::new(*page_no, image))
            .collect()
    }

    #[test]
    fn identity_binds_page_number_size_count_and_contents() {
        let first = cache(&[page(1), page(2)]).identity();
        let reordered = cache(&[page(2), page(1)]).identity();
        let changed = cache(&[page(1), page(3)]).identity();
        let shorter = cache(&[page(1)]).identity();
        let larger_pages = PageStateCacheV3::from_pages(1024, [vec![1; 1024], vec![2; 1024]])
            .unwrap()
            .identity();

        assert_ne!(first, reordered);
        assert_ne!(first, changed);
        assert_ne!(first.state_root, shorter.state_root);
        assert_ne!(first.state_root, larger_pages.state_root);
    }

    #[test]
    fn overlay_is_non_mutating_and_apply_matches_full_rebuild() {
        let mut pages = vec![page(1), page(2), page(3), page(4)];
        let mut state = cache(&pages);
        let before = state.clone();
        let changed = vec![(2, page(8)), (4, page(9))];
        let patch = patches(&changed);

        let overlaid = state.overlay(4, &patch).unwrap();
        assert_eq!(state, before);
        pages[1] = changed[0].1.clone();
        pages[3] = changed[1].1.clone();
        assert_eq!(overlaid, cache(&pages).identity());
        assert_eq!(state.apply_patch(4, &patch).unwrap(), overlaid);
        assert_eq!(state.identity(), cache(&pages).identity());
    }

    #[test]
    fn growth_requires_the_complete_new_suffix_without_mutating_on_error() {
        let mut state = cache(&[page(1), page(2)]);
        let before = state.clone();
        let incomplete = vec![(3, page(3)), (5, page(5))];

        assert!(state.apply_patch(5, &patches(&incomplete)).is_err());
        assert_eq!(state, before);

        let complete = vec![(3, page(3)), (4, page(4)), (5, page(5))];
        let identity = state.apply_patch(5, &patches(&complete)).unwrap();
        assert_eq!(
            identity,
            cache(&[page(1), page(2), page(3), page(4), page(5)]).identity()
        );
    }

    #[test]
    fn shrink_prunes_removed_pages_before_later_growth() {
        let mut state = cache(&[page(1), page(2), page(3), page(4), page(5)]);
        let shrunk = state.apply_patch(2, &[]).unwrap();
        assert_eq!(shrunk, cache(&[page(1), page(2)]).identity());

        let replacement = vec![(3, page(9)), (4, page(8)), (5, page(7))];
        let regrown = state.apply_patch(5, &patches(&replacement)).unwrap();
        assert_eq!(
            regrown,
            cache(&[page(1), page(2), page(9), page(8), page(7)]).identity()
        );
    }

    #[test]
    fn invalid_patch_shapes_fail_closed() {
        let cases = [
            vec![(0, page(9))],
            vec![(2, page(8)), (1, page(9))],
            vec![(1, page(8)), (1, page(9))],
            vec![(4, page(9))],
            vec![(1, vec![0; PAGE_SIZE - 1])],
        ];

        for malformed in &cases {
            let mut state = cache(&[page(1), page(2), page(3)]);
            let before = state.clone();
            assert!(state.apply_patch(3, &patches(malformed)).is_err());
            assert_eq!(state, before);
        }
    }

    #[test]
    fn rebuild_rejects_invalid_page_geometry() {
        assert!(PageStateCacheV3::from_pages(511, [vec![0; 511]]).is_err());
        assert!(PageStateCacheV3::from_pages(512, Vec::<Vec<u8>>::new()).is_err());
        assert!(PageStateCacheV3::from_pages(512, [vec![0; 511]]).is_err());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn incremental_identity_matches_full_rebuild_across_mixed_changes(
            initial_len in 1usize..8,
            initial_seed in any::<u8>(),
            operations in prop::collection::vec((0u8..3, any::<u8>(), 0usize..16), 1..40),
        ) {
            let mut pages: Vec<Vec<u8>> = (0..initial_len)
                .map(|index| page(initial_seed.wrapping_add(index as u8)))
                .collect();
            let mut state = cache(&pages);

            for (kind, byte, hint) in operations {
                let changed = match kind {
                    0 => {
                        let index = hint % pages.len();
                        pages[index] = page(byte);
                        vec![(index as u32 + 1, pages[index].clone())]
                    }
                    1 if pages.len() < 16 => {
                        let added = 1 + hint % (16 - pages.len()).min(3);
                        let first = pages.len();
                        pages.extend((0..added).map(|offset| page(byte.wrapping_add(offset as u8))));
                        pages[first..]
                            .iter()
                            .enumerate()
                            .map(|(offset, image)| ((first + offset) as u32 + 1, image.clone()))
                            .collect()
                    }
                    _ if pages.len() > 1 => {
                        let target = 1 + hint % (pages.len() - 1);
                        pages.truncate(target);
                        Vec::new()
                    }
                    _ => {
                        pages[0] = page(byte);
                        vec![(1, pages[0].clone())]
                    }
                };
                let patch = patches(&changed);
                let rebuilt = cache(&pages).identity();

                prop_assert_eq!(state.overlay(pages.len() as u32, &patch).unwrap(), rebuilt);
                prop_assert_eq!(state.apply_patch(pages.len() as u32, &patch).unwrap(), rebuilt);
                prop_assert_eq!(state.identity(), rebuilt);
            }
        }
    }
}
