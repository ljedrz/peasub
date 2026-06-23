//! Shared, clone-able state backing every [`Node`].
//!
//! [`Node`]: crate::Node

use lru::LruCache;
use parking_lot::Mutex;

use peashape::ID_SIZE;

/// The shared, node-wide state that backs every clone of a
/// [`Node`].
///
/// The gossip-specific bookkeeping that lives on top of
/// `peashape` is just the LRU dedup cache: an [`LruCache`] of
/// recently-seen message identifiers used to suppress
/// re-broadcast of frames that have already circulated through
/// this node. The priority lanes, the incoming broadcast, and
/// the cover-traffic scheduler all live in
/// [`peashape::Shaper`]; see `peashape` for their semantics.
///
/// [`Node`]: crate::Node
pub(crate) struct GossipState {
    seen: Mutex<LruCache<[u8; ID_SIZE], ()>>,
}

impl GossipState {
    /// Builds a fresh `GossipState`.
    ///
    /// # Panics
    ///
    /// Panics if `dedup_capacity` is `0` (the LRU constructor
    /// requires a non-zero capacity).
    pub(crate) fn new(dedup_capacity: usize) -> Self {
        Self {
            seen: Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(dedup_capacity)
                    .expect("dedup_capacity must be non-zero"),
            )),
        }
    }

    /// Returns `true` if `id` has not been seen before (and
    /// records it in the LRU); `false` if it has. Callers use
    /// this to deduplicate incoming frames.
    pub(crate) fn check_and_record(&self, id: &[u8; ID_SIZE]) -> bool {
        let mut seen = self.seen.lock();
        if seen.contains(id) {
            return false;
        }
        seen.put(*id, ());
        true
    }
}
