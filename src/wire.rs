//! Experimental message framing, independent of the payload codec and transport.
//! The transport owns message boundaries and binds a unique authenticated Session
//! incarnation. It must not rebind old messages to a new session after reconnect.
//! Encoding can keep the header on the stack and borrow the shared payload.

use crate::{Envelope, Receipt, Session};
use std::io::IoSlice;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireError {
    Truncated,
    InvalidFlags,
    InvalidInteger,
    InvalidEnvelope,
    PayloadLimit,
    TrailingBytes,
}
impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for WireError {}

fn put(mut value: u64, out: &mut [u8], at: &mut usize) {
    while value >= 128 {
        out[*at] = (value as u8 & 127) | 128;
        *at += 1;
        value >>= 7;
    }
    out[*at] = value as u8;
    *at += 1;
}
fn take(bytes: &[u8], at: &mut usize) -> Result<u64, WireError> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*at).ok_or(WireError::Truncated)?;
        *at += 1;
        if shift == 63 && byte > 1 {
            return Err(WireError::InvalidInteger);
        }
        value |= ((byte & 127) as u64) << shift;
        if byte < 128 {
            if shift != 0 && byte == 0 {
                return Err(WireError::InvalidInteger);
            }
            return Ok(value);
        }
    }
    Err(WireError::InvalidInteger)
}

/// At most 31 bytes: v1 flags and three u64 varints. Session is transport context.
pub struct Header {
    bytes: [u8; 31],
    length: usize,
}
impl Header {
    pub fn new(receipt: Receipt, baseline: Option<u64>) -> Result<Self, WireError> {
        if receipt.attempt == 0 || baseline.is_some_and(|base| base > receipt.revision) {
            return Err(WireError::InvalidEnvelope);
        }
        let mut header = Self {
            bytes: [0; 31],
            length: 1,
        };
        header.bytes[0] = 0x10 | u8::from(baseline.is_some());
        put(receipt.attempt, &mut header.bytes, &mut header.length);
        put(receipt.revision, &mut header.bytes, &mut header.length);
        if let Some(base) = baseline {
            put(base, &mut header.bytes, &mut header.length);
        }
        Ok(header)
    }
    pub fn for_envelope(envelope: &Envelope) -> Result<Self, WireError> {
        Self::new(envelope.receipt, envelope.baseline)
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
    /// Header and body remain borrowed until the transport has consumed them.
    pub fn slices<'a>(&'a self, body: &'a [u8]) -> [IoSlice<'a>; 2] {
        [IoSlice::new(self.as_bytes()), IoSlice::new(body)]
    }
    /// Append one complete message to a caller-owned buffer.
    pub fn append(&self, body: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(self.as_bytes());
        out.extend_from_slice(body);
    }
}

#[derive(Debug)]
pub struct Decoded<'a> {
    pub receipt: Receipt,
    pub baseline: Option<u64>,
    pub body: &'a [u8],
}
pub fn decode(
    session: Session,
    message: &[u8],
    max_payload: usize,
) -> Result<Decoded<'_>, WireError> {
    let flags = *message.first().ok_or(WireError::Truncated)?;
    if flags != 0x10 && flags != 0x11 {
        return Err(WireError::InvalidFlags);
    }
    let mut at = 1;
    let attempt = take(message, &mut at)?;
    let revision = take(message, &mut at)?;
    let baseline = if flags == 0x11 {
        Some(take(message, &mut at)?)
    } else {
        None
    };
    if attempt == 0 || baseline.is_some_and(|b| b > revision) {
        return Err(WireError::InvalidEnvelope);
    }
    let body = &message[at..];
    if body.len() > max_payload {
        return Err(WireError::PayloadLimit);
    }
    Ok(Decoded {
        receipt: Receipt {
            session,
            attempt,
            revision,
        },
        baseline,
        body,
    })
}

pub struct Ack {
    bytes: [u8; 10],
    length: usize,
}
impl Ack {
    pub fn new(attempt: u64) -> Result<Self, WireError> {
        if attempt == 0 {
            return Err(WireError::InvalidEnvelope);
        }
        let mut ack = Self {
            bytes: [0; 10],
            length: 0,
        };
        put(attempt, &mut ack.bytes, &mut ack.length);
        Ok(ack)
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}
/// Parse a complete ACK message containing one or more canonical attempt IDs.
pub struct Acks<'a> {
    bytes: &'a [u8],
}
impl<'a> Acks<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}
impl Iterator for Acks<'_> {
    type Item = Result<u64, WireError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() {
            return None;
        }
        let mut at = 0;
        let result = take(self.bytes, &mut at).and_then(|n| {
            if n == 0 {
                Err(WireError::InvalidEnvelope)
            } else {
                Ok(n)
            }
        });
        self.bytes = if result.is_ok() {
            &self.bytes[at..]
        } else {
            &[]
        };
        Some(result)
    }
}
pub fn decode_ack(bytes: &[u8]) -> Result<u64, WireError> {
    let mut acks = Acks::new(bytes);
    let first = acks.next().ok_or(WireError::Truncated)??;
    if acks.next().is_some() {
        return Err(WireError::TrailingBytes);
    }
    Ok(first)
}
