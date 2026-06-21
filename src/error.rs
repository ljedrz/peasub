//! Error types returned by the public API.

use thiserror::Error;

/// All errors that can be surfaced to the application through the
/// public `peasub` API.
#[derive(Debug, Error)]
pub enum Error {
    /// An I/O error from the underlying `pea2pea` transport, e.g. a
    /// failed `connect`, `bind`, or socket read.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The application payload is too large to fit in a single gossip
    /// frame. The maximum size is `message_size - ID_SIZE` bytes
    /// (the rest of the frame is the message identifier plus random
    /// padding). Either shrink the payload or raise `message_size`.
    #[error("payload too large: {size} bytes, maximum is {max}")]
    PayloadTooLarge { size: usize, max: usize },

    /// The application outbox is full. This happens when `publish` is
    /// called faster than the cover rate for a sustained period, so
    /// the queue of pending application messages has reached
    /// `app_outbox_capacity`. The caller should either slow down,
    /// raise the cover rate, or raise `app_outbox_capacity`.
    #[error("application outbox is full; the cover rate is too low for the current publish rate")]
    AppOutboxFull,
}
