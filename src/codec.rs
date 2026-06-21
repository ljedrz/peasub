use std::io;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// A `pea2pea` codec that frames every gossip message to a fixed on-the-wire
/// size, regardless of the actual payload contents.
///
/// All frames consist of a 4-byte big-endian length prefix followed by
/// exactly `expected_size` payload bytes. The combination of constant
/// length and constant payload size is what makes cover traffic
/// indistinguishable from real traffic on the wire.
pub struct Codec {
    inner: LengthDelimitedCodec,
    expected_size: usize,
}

impl Codec {
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
