use rill_replicate::{bytes::ByteRuns, Codec};
use std::io::{Cursor, Read};

pub const LIMIT: usize = 1024 * 1024;
enum Backend {
    Full,
    Runs(ByteRuns),
    Bsdiff,
    Qbsdiff(bool),
    Bidiff(bool),
}
pub struct Adapter {
    backend: Backend,
    compressed: bool,
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
impl Adapter {
    pub fn new(name: &str) -> Self {
        let (compressed, name) = name
            .strip_prefix("lz4+")
            .map_or((false, name), |n| (true, n));
        let backend = match name {
            "full" => Backend::Full,
            "bsdiff" => Backend::Bsdiff,
            "qbsdiff-default" => Backend::Qbsdiff(false),
            "qbsdiff-small" => Backend::Qbsdiff(true),
            "bidiff-default" => Backend::Bidiff(false),
            "bidiff-single" => Backend::Bidiff(true),
            _ => {
                let (block, gap) = name
                    .strip_prefix("runs-b")
                    .unwrap()
                    .split_once("-g")
                    .unwrap();
                Backend::Runs(
                    ByteRuns::new(block.parse().unwrap(), gap.parse().unwrap(), LIMIT).unwrap(),
                )
            }
        };
        Self {
            backend,
            compressed,
        }
    }
    fn pack(&self, bytes: Vec<u8>) -> Vec<u8> {
        if self.compressed {
            lz4_flex::compress_prepend_size(&bytes)
        } else {
            bytes
        }
    }
    fn unpack(&self, bytes: &[u8]) -> Result<Vec<u8>, String> {
        if self.compressed {
            let size = u32::from_le_bytes(bytes.get(..4).ok_or("lz4 header")?.try_into().unwrap())
                as usize;
            if size > LIMIT * 4 {
                return Err("encoded limit".into());
            }
            lz4_flex::decompress_size_prepended(bytes).map_err(err)
        } else {
            Ok(bytes.to_vec())
        }
    }
    pub fn delta(&self, base: &[u8], state: &[u8]) -> Result<Vec<u8>, String> {
        // bidiff's suffix matcher panics for an empty source. Retain a native
        // capability proof and use an explicit full fallback, never a fake delta.
        if matches!(self.backend, Backend::Full)
            || (base.is_empty() && matches!(self.backend, Backend::Bidiff(_)))
        {
            let mut full = Vec::with_capacity(state.len() + 1);
            full.push(0);
            full.extend_from_slice(state);
            return Ok(self.pack(full));
        }
        let mut out = Vec::new();
        match self.backend {
            Backend::Full => out.extend_from_slice(state),
            Backend::Runs(codec) => out = codec.diff(base, state).map_err(|e| format!("{e:?}"))?,
            Backend::Bsdiff => bsdiff::diff(base, state, &mut out).map_err(err)?,
            Backend::Qbsdiff(small) => {
                let diff = qbsdiff::Bsdiff::new(base, state);
                let diff = if small {
                    diff.compression_level(1)
                        .parallel_scheme(qbsdiff::ParallelScheme::Never)
                } else {
                    diff
                };
                diff.compare(&mut out).map_err(err)?;
            }
            Backend::Bidiff(single) => {
                if single {
                    bidiff::simple_diff_with_params(
                        base,
                        state,
                        &mut out,
                        &bidiff::DiffParams::new(1, None).map_err(err)?,
                    )
                    .map_err(err)?;
                } else {
                    bidiff::simple_diff(base, state, &mut out).map_err(err)?;
                }
            }
        }
        // The same delta/full tag is charged to every adapter, before LZ4.
        out.insert(0, 1);
        Ok(self.pack(out))
    }
    pub fn apply(&self, base: &[u8], bytes: &[u8]) -> Result<Vec<u8>, String> {
        let data;
        let bytes = if self.compressed {
            data = self.unpack(bytes)?;
            &data[..]
        } else {
            bytes
        };
        let (&tag, bytes) = bytes.split_first().ok_or("empty envelope")?;
        if tag == 0 {
            if bytes.len() > LIMIT {
                return Err("output limit".into());
            }
            return Ok(bytes.to_vec());
        }
        if tag != 1 {
            return Err("delta tag".into());
        }
        let mut out = Vec::new();
        match self.backend {
            Backend::Full => out.extend_from_slice(bytes),
            Backend::Runs(codec) => return codec.patch(base, bytes).map_err(|e| format!("{e:?}")),
            Backend::Bsdiff => bsdiff::patch(base, &mut &bytes[..], &mut out).map_err(err)?,
            Backend::Qbsdiff(small) => {
                let patch = qbsdiff::Bspatch::new(bytes).map_err(err)?;
                if patch.hint_target_size() > LIMIT as u64 {
                    return Err("output limit".into());
                }
                let patch = if small {
                    patch.buffer_size(4096).delta_min(128)
                } else {
                    patch
                };
                patch.apply(base, &mut out).map_err(err)?;
            }
            Backend::Bidiff(_) => {
                bipatch::Reader::new(bytes, Cursor::new(base))
                    .map_err(err)?
                    .take((LIMIT + 1) as u64)
                    .read_to_end(&mut out)
                    .map_err(err)?;
            }
        }
        if out.len() > LIMIT {
            return Err("output limit".into());
        }
        Ok(out)
    }
}
impl Codec<Vec<u8>> for Adapter {
    type Error = String;
    fn encode_full(&self, state: &Vec<u8>) -> Result<Vec<u8>, String> {
        Ok(self.pack(state.clone()))
    }
    fn decode_full(&self, bytes: &[u8]) -> Result<Vec<u8>, String> {
        let out = self.unpack(bytes)?;
        if out.len() > LIMIT {
            Err("output limit".into())
        } else {
            Ok(out)
        }
    }
    fn encode_delta(&self, base: &Vec<u8>, state: &Vec<u8>) -> Result<Vec<u8>, String> {
        self.delta(base, state)
    }
    fn decode_delta(&self, base: &Vec<u8>, bytes: &[u8]) -> Result<Vec<u8>, String> {
        self.apply(base, bytes)
    }
}

pub fn random(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}
pub fn initial(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| (i as u64 / 8).to_le_bytes()[i % 8])
        .collect()
}
pub fn mutate(state: &mut Vec<u8>, case: &str, tick: usize, seed: &mut u64) {
    match case {
        "sparse" => {
            if !state.is_empty() {
                let at = random(seed) as usize % state.len();
                state[at] = state[at].wrapping_add(1);
            }
        }
        "dense" => {
            for (i, v) in state.iter_mut().enumerate() {
                *v = (tick as u64 * 777 + (i / 8) as u64).to_le_bytes()[i % 8];
            }
        }
        "random" => {
            for v in state.iter_mut() {
                *v = random(seed) as u8;
            }
        }
        "shift" => {
            if !state.is_empty() {
                state.rotate_right(1);
                state[0] = random(seed) as u8;
            }
        }
        "resize" => {
            if tick % 2 == 0 {
                state.truncate(state.len().saturating_sub(17));
            } else {
                for _ in 0..23 {
                    state.push(random(seed) as u8);
                }
            }
        }
        _ => panic!("case"),
    }
}
