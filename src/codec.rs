//! Length-delimited framing for `peasub` frames.

use std::io;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// A `pea2pea` codec that constrains every gossip frame on the wire
/// to a single, fixed size, regardless of the actual payload
/// contents.
///
/// # Wire format
///
/// ```text
/// +--------+----------------+
/// | length |     payload    |
/// | (4 B)  |  (expected_size)|
/// +--------+----------------+
/// ```
///
/// - `length`: a 4-byte big-endian unsigned integer. Always equal
///   to `expected_size` for a well-formed frame; any other value
///   causes the connection to be torn down by the decoder.
/// - `payload`: exactly `expected_size` bytes of frame data. Real
///   gossip messages and cover messages are byte-for-byte
///   indistinguishable inside this field, which is what makes
///   traffic-analysis resistance work: an observer cannot tell
///   "what" is being sent, only "when" and "to whom" and "how
///   big" (and the last two are constant).
pub struct Codec {
    inner: LengthDelimitedCodec,
    expected_size: usize,
}

impl Codec {
    /// Creates a codec that accepts frames of exactly
    /// `expected_size` payload bytes and rejects any frame larger
    /// than `max_frame_size` bytes (defensive DoS bound).
    pub fn new(expected_size: usize, max_frame_size: usize) -> Self {
        let inner = LengthDelimitedCodec::builder()
            .max_frame_length(max_frame_size)
            .length_field_length(4)
            .big_endian()
            .new_codec();
        Self {
            inner,
            expected_size,
        }
    }
}

impl Decoder for Codec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src)? {
            Some(buf) => {
                if buf.len() != self.expected_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "unexpected gossip frame size: got {}, want {}",
                            buf.len(),
                            self.expected_size
                        ),
                    ));
                }
                Ok(Some(buf))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<BytesMut> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.len() != self.expected_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "unexpected gossip frame size: got {}, want {}",
                    item.len(),
                    self.expected_size
                ),
            ));
        }
        // `LengthDelimitedCodec::encode` is generic over `Into<Bytes>`;
        // freeze our `BytesMut` so it is taken by-value without a copy.
        self.inner.encode(item.freeze(), dst)
    }
}
