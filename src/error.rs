use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("payload too large: {size} bytes, maximum is {max}")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("application outbox is full; the cover rate is too low for the current publish rate")]
    AppOutboxFull,

    #[error("node is shutting down")]
    ShuttingDown,

    #[error("expected listener, but the node is not configured to listen")]
    NoListener,
}
