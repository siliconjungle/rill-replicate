use rill_replicate::Codec;
use std::collections::BTreeMap;

pub type State = BTreeMap<u64, u64>;

/// Fixed-width demonstration codec, not a proposed production wire format.
pub struct MapCodec;

fn word(bytes: &[u8], offset: usize) -> Result<u64, &'static str> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or("truncated")?
            .try_into()
            .unwrap(),
    ))
}

impl Codec<State> for MapCodec {
    type Error = &'static str;
    fn encode_full(&self, state: &State) -> Result<Vec<u8>, Self::Error> {
        let mut out = Vec::with_capacity(8 + state.len() * 16);
        out.extend_from_slice(&(state.len() as u64).to_le_bytes());
        for (k, v) in state {
            out.extend_from_slice(&k.to_le_bytes());
            out.extend_from_slice(&v.to_le_bytes());
        }
        Ok(out)
    }
    fn encode_delta(&self, base: &State, state: &State) -> Result<Vec<u8>, Self::Error> {
        let mut out = vec![0; 8];
        let mut count = 0u64;
        for (key, value) in state {
            if base.get(key) != Some(value) {
                out.push(1);
                out.extend_from_slice(&key.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
                count += 1;
            }
        }
        for key in base.keys() {
            if !state.contains_key(key) {
                out.push(0);
                out.extend_from_slice(&key.to_le_bytes());
                out.extend_from_slice(&0u64.to_le_bytes());
                count += 1;
            }
        }
        out[..8].copy_from_slice(&count.to_le_bytes());
        Ok(out)
    }
    fn decode_full(&self, bytes: &[u8]) -> Result<State, Self::Error> {
        let count = usize::try_from(word(bytes, 0)?).map_err(|_| "count")?;
        if bytes.len().checked_sub(8) != count.checked_mul(16) {
            return Err("length");
        }
        let mut state = State::new();
        for i in 0..count {
            if state
                .insert(word(bytes, 8 + i * 16)?, word(bytes, 16 + i * 16)?)
                .is_some()
            {
                return Err("duplicate");
            }
        }
        Ok(state)
    }
    fn decode_delta(&self, base: &State, bytes: &[u8]) -> Result<State, Self::Error> {
        let count = usize::try_from(word(bytes, 0)?).map_err(|_| "count")?;
        if bytes.len().checked_sub(8) != count.checked_mul(17) {
            return Err("length");
        }
        let mut state = base.clone();
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..count {
            let at = 8 + i * 17;
            let key = word(bytes, at + 1)?;
            if !seen.insert(key) {
                return Err("duplicate");
            }
            match bytes[at] {
                0 if word(bytes, at + 9)? == 0 => {
                    state.remove(&key);
                }
                1 => {
                    state.insert(key, word(bytes, at + 9)?);
                }
                _ => return Err("opcode"),
            }
        }
        Ok(state)
    }
}

pub const STRATEGIES: [rill_replicate::Strategy; 5] = [
    rill_replicate::Strategy::Full,
    rill_replicate::Strategy::SharedFull,
    rill_replicate::Strategy::PerRecipientDelta,
    rill_replicate::Strategy::SharedDelta,
    rill_replicate::Strategy::SharedAdaptive,
];

pub fn initial(n: u64) -> State {
    (0..n).map(|k| (k, k * 17)).collect()
}
