//! Experimental generic byte-run codec, with a fixed configuration per session.
//! No compression, hashing, schema or platform-dependent wire integers. Run
//! offsets/lengths are unsigned varints; the target length is little-endian u64.
//! A configured output limit is checked before allocating decoded output.

use crate::Codec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteError {
    Limit,
    Malformed,
}

/// Compares fixed blocks and merges changed runs across at most `merge_gap`
/// unchanged bytes. Large blocks trade extra bytes for fewer comparisons/headers.
/// This opt-in codec is not a selected universal default or a stable wire format.
#[derive(Debug, Clone, Copy)]
pub struct ByteRuns {
    block: usize,
    merge_gap: usize,
    max_output: usize,
}

fn varint(mut value: usize, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

fn take_varint(bytes: &[u8], at: &mut usize) -> Result<usize, ByteError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*at).ok_or(ByteError::Malformed)?;
        *at += 1;
        if shift == 63 && byte > 1 {
            return Err(ByteError::Malformed);
        }
        value |= ((byte & 127) as u64) << shift;
        if byte < 128 {
            if shift != 0 && byte == 0 {
                return Err(ByteError::Malformed);
            }
            return usize::try_from(value).map_err(|_| ByteError::Malformed);
        }
    }
    Err(ByteError::Malformed)
}

impl ByteRuns {
    pub fn new(block: usize, merge_gap: usize, max_output: usize) -> Option<Self> {
        (block != 0).then_some(Self {
            block,
            merge_gap,
            max_output,
        })
    }
    fn run(state: &[u8], start: usize, end: usize, previous: &mut usize, out: &mut Vec<u8>) {
        varint(start - *previous, out);
        varint(end - start, out);
        out.extend_from_slice(&state[start..end]);
        *previous = end;
    }
    pub fn diff(&self, base: &[u8], state: &[u8]) -> Result<Vec<u8>, ByteError> {
        if state.len() > self.max_output {
            return Err(ByteError::Limit);
        }
        let mut out = Vec::with_capacity(12);
        out.extend_from_slice(b"RR01");
        out.extend_from_slice(&(state.len() as u64).to_le_bytes());
        let mut pending: Option<(usize, usize)> = None;
        let mut previous = 0usize;
        for (index, chunk) in state.chunks(self.block).enumerate() {
            let start = index * self.block;
            let end = start + chunk.len();
            if base.get(start..end) == Some(chunk) {
                continue;
            }
            match pending {
                Some((first, last)) if start - last <= self.merge_gap => {
                    pending = Some((first, end))
                }
                Some((first, last)) => {
                    Self::run(state, first, last, &mut previous, &mut out);
                    pending = Some((start, end));
                }
                None => pending = Some((start, end)),
            }
        }
        if let Some((first, last)) = pending {
            Self::run(state, first, last, &mut previous, &mut out);
        }
        Ok(out)
    }
    pub fn patch(&self, base: &[u8], bytes: &[u8]) -> Result<Vec<u8>, ByteError> {
        if bytes.get(..4) != Some(b"RR01") {
            return Err(ByteError::Malformed);
        }
        let size = u64::from_le_bytes(
            bytes
                .get(4..12)
                .ok_or(ByteError::Malformed)?
                .try_into()
                .unwrap(),
        );
        let size = usize::try_from(size).map_err(|_| ByteError::Limit)?;
        if size > self.max_output {
            return Err(ByteError::Limit);
        }
        // Validate the whole patch before allocating/copying application state.
        let mut at = 12;
        let mut previous = 0usize;
        while at < bytes.len() {
            let start = previous
                .checked_add(take_varint(bytes, &mut at)?)
                .ok_or(ByteError::Malformed)?;
            let len = take_varint(bytes, &mut at)?;
            let end = start.checked_add(len).ok_or(ByteError::Malformed)?;
            if len == 0 || end > size {
                return Err(ByteError::Malformed);
            }
            at = at
                .checked_add(len)
                .filter(|n| *n <= bytes.len())
                .ok_or(ByteError::Malformed)?;
            previous = end;
        }
        let mut state = base[..base.len().min(size)].to_vec();
        state.resize(size, 0);
        at = 12;
        previous = 0;
        while at < bytes.len() {
            let start = previous + take_varint(bytes, &mut at)?;
            let end = start + take_varint(bytes, &mut at)?;
            state[start..end].copy_from_slice(&bytes[at..at + end - start]);
            at += end - start;
            previous = end;
        }
        Ok(state)
    }
}

impl Codec<Vec<u8>> for ByteRuns {
    type Error = ByteError;
    fn encode_full(&self, state: &Vec<u8>) -> Result<Vec<u8>, ByteError> {
        if state.len() > self.max_output {
            Err(ByteError::Limit)
        } else {
            Ok(state.clone())
        }
    }
    fn encode_delta(&self, base: &Vec<u8>, state: &Vec<u8>) -> Result<Vec<u8>, ByteError> {
        self.diff(base, state)
    }
    fn decode_full(&self, bytes: &[u8]) -> Result<Vec<u8>, ByteError> {
        if bytes.len() > self.max_output {
            Err(ByteError::Limit)
        } else {
            Ok(bytes.to_vec())
        }
    }
    fn decode_delta(&self, base: &Vec<u8>, bytes: &[u8]) -> Result<Vec<u8>, ByteError> {
        self.patch(base, bytes)
    }
}
