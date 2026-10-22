//! Development comparison. Receiver verification is outside sender timings.
use rill_replicate::{
    Applied, Codec, Config, Envelope, Error, Receiver, Sender, Session, Strategy,
};
use std::time::Instant;

struct Words {
    mask: bool,
}
fn read(bytes: &[u8]) -> Result<Vec<u64>, &'static str> {
    if bytes.len() % 8 != 0 {
        return Err("length");
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .collect())
}
fn encode(words: impl Iterator<Item = u64>) -> Vec<u8> {
    words.flat_map(u64::to_le_bytes).collect()
}
impl Codec<Vec<u64>> for Words {
    type Error = &'static str;
    fn encode_full(&self, state: &Vec<u64>) -> Result<Vec<u8>, Self::Error> {
        Ok(encode(
            std::iter::once(state.len() as u64).chain(state.iter().copied()),
        ))
    }
    fn decode_full(&self, bytes: &[u8]) -> Result<Vec<u64>, Self::Error> {
        let words = read(bytes)?;
        if words.is_empty() || words[0] != (words.len() - 1) as u64 {
            return Err("length");
        }
        Ok(words[1..].to_vec())
    }
    fn encode_delta(&self, base: &Vec<u64>, state: &Vec<u64>) -> Result<Vec<u8>, Self::Error> {
        if base.len() != state.len() {
            return Err("shape");
        }
        let mut words = vec![state.len() as u64];
        if self.mask {
            words.resize(1 + state.len().div_ceil(64), 0);
            for (i, v) in state.iter().enumerate() {
                if base[i] != *v {
                    words[1 + i / 64] |= 1 << (i % 64);
                    words.push(*v);
                }
            }
        } else {
            for (i, v) in state.iter().enumerate() {
                if base[i] != *v {
                    words.extend([i as u64, *v]);
                }
            }
        }
        Ok(encode(words.into_iter()))
    }
    fn decode_delta(&self, base: &Vec<u64>, bytes: &[u8]) -> Result<Vec<u64>, Self::Error> {
        let words = read(bytes)?;
        if words.first() != Some(&(base.len() as u64)) {
            return Err("shape");
        }
        let mut out = base.clone();
        if self.mask {
            let masks = base.len().div_ceil(64);
            if words.len() < 1 + masks {
                return Err("mask");
            }
            if base.len() % 64 != 0 && words[masks] >> (base.len() % 64) != 0 {
                return Err("mask");
            }
            let mut at = 1 + masks;
            for (i, value) in out.iter_mut().enumerate() {
                if words[1 + i / 64] & (1 << (i % 64)) != 0 {
                    *value = *words.get(at).ok_or("value")?;
                    at += 1;
                }
            }
            if at != words.len() {
                return Err("length");
            }
        } else {
            if words.len() % 2 != 1 {
                return Err("pairs");
            }
            let mut previous = None;
            for pair in words[1..].chunks_exact(2) {
                let i = usize::try_from(pair[0]).map_err(|_| "index")?;
                if i >= out.len() || previous.is_some_and(|p| p >= i) {
                    return Err("index");
                }
                out[i] = pair[1];
                previous = Some(i);
            }
        }
        Ok(out)
    }
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(
        args.len(),
        7,
        "strategy codec history cache peers frames case"
    );
    let strategy = match args[0].as_str() {
        "full" => Strategy::Full,
        "shared-full" => Strategy::SharedFull,
        "individual" => Strategy::PerRecipientDelta,
        "shared" => Strategy::SharedDelta,
        "adaptive" => Strategy::SharedAdaptive,
        _ => panic!("strategy"),
    };
    let codec = Words {
        mask: match args[1].as_str() {
            "index" => false,
            "mask" => true,
            _ => panic!("codec"),
        },
    };
    let history: usize = args[2].parse().unwrap();
    let cache: usize = args[3].parse().unwrap();
    let peers: usize = args[4].parse().unwrap();
    let frames: usize = args[5].parse().unwrap();
    let case = args[6].as_str();
    assert!(matches!(case, "sync" | "staggered" | "loss" | "dense"));
    let cfg = Config {
        history,
        peers,
        attempts_per_peer: 8,
        cache_entries: cache,
        cache_bytes: 1024 * 1024,
    };
    let mut expected: Vec<u64> = (0..256).collect();
    let mut model = vec![expected.clone()];
    let mut sender =
        Sender::new(expected.clone(), Words { mask: codec.mask }, cfg, strategy).unwrap();
    let mut receivers: Vec<_> = (1..=peers)
        .map(|id| {
            sender.add_peer(Session(id as u128)).unwrap();
            Receiver::new(Session(id as u128), history).unwrap()
        })
        .collect();
    let mut sender_ns = 0;
    let mut logical_bytes = 0usize;
    let mut applied = 0;
    let mut missing = 0;
    let mut delayed: Vec<(usize, Envelope)> = Vec::new();
    let mut lag_sum = 0u64;
    let mut max_lag = 0;
    let start = Instant::now();
    // Two extra final full rounds guarantee convergence after loss/stalls.
    for tick in 0..frames + 2 {
        if tick > 0 && tick < frames {
            if case == "dense" {
                for (i, v) in expected.iter_mut().enumerate() {
                    *v = (tick as u64) * 777 + i as u64;
                }
            } else {
                expected[tick % 256] = tick as u64 * 9999;
            }
            model.push(expected.clone());
            let t = Instant::now();
            sender.publish(expected.clone()).unwrap();
            sender_ns += t.elapsed().as_nanos();
        }
        let mut packets = Vec::with_capacity(peers);
        let t = Instant::now();
        if tick == frames {
            for id in 1..=peers {
                sender.resynchronize(Session(id as u128)).unwrap();
            }
        }
        for peer in 0..peers {
            // Independent slow groups generate multiple real acknowledged bases.
            if tick < frames
                && ((case == "staggered" && tick % (1 + peer % 8) != 0)
                    || (case == "loss" && peer % 97 == 0 && tick > 0))
            {
                continue;
            }
            let packet = sender.prepare(Session(peer as u128 + 1)).unwrap();
            packets.push((peer, packet));
        }
        sender_ns += t.elapsed().as_nanos();
        let mut incoming = Vec::new();
        for (peer, packet) in packets {
            logical_bytes += packet.body.len();
            if case == "loss" && tick < frames {
                if (peer + tick) % 11 == 0 {
                    continue;
                }
                if (peer + tick) % 13 == 0 {
                    delayed.push((peer, packet));
                    continue;
                }
                if (peer + tick) % 17 == 0 {
                    logical_bytes += packet.body.len();
                    incoming.push((peer, packet.clone()));
                }
            }
            incoming.push((peer, packet));
        }
        if tick % 3 == 0 || tick >= frames {
            incoming.append(&mut delayed);
        }
        incoming.reverse();
        for (peer, packet) in incoming {
            match receivers[peer].apply(&packet, &codec) {
                Ok(Applied::Updated) => {
                    assert_eq!(
                        receivers[peer].current(),
                        Some(&model[packet.receipt.revision as usize])
                    );
                    applied += 1;
                    if case != "loss" || tick >= frames || (peer + tick) % 5 != 0 {
                        let t = Instant::now();
                        sender.acknowledge(packet.receipt);
                        sender_ns += t.elapsed().as_nanos();
                    }
                }
                Ok(Applied::Stale) => {
                    // A duplicate of the exact current state was already applied.
                    if receivers[peer].revision() == Some(packet.receipt.revision) {
                        let t = Instant::now();
                        sender.acknowledge(packet.receipt);
                        sender_ns += t.elapsed().as_nanos();
                    }
                }
                Err(Error::MissingBaseline(_)) => {
                    missing += 1;
                    let t = Instant::now();
                    sender.resynchronize(packet.receipt.session).unwrap();
                    sender_ns += t.elapsed().as_nanos();
                }
                other => panic!("{other:?}"),
            }
        }
        for receiver in &receivers {
            let lag = sender
                .revision()
                .saturating_sub(receiver.revision().unwrap_or(0));
            lag_sum += lag;
            max_lag = max_lag.max(lag);
            assert!(receiver.retained_revisions() <= history);
        }
        assert!(sender.retained_revisions() <= history);
        assert!(sender.cached_entries() <= cache);
        assert!(sender.cached_bytes() <= cfg.cache_bytes);
        assert!(sender.pending_attempts() <= peers * 8);
    }
    for (peer, receiver) in receivers.iter().enumerate() {
        assert_eq!(receiver.current(), Some(&expected));
        assert_eq!(
            sender.acknowledged(Session(peer as u128 + 1)),
            Some(sender.revision())
        );
    }
    let s = sender.stats();
    println!("{{\"strategy\":\"{}\",\"codec\":\"{}\",\"history\":{},\"cache\":{},\"peers\":{},\"frames\":{},\"case\":\"{}\",\"sender_ns\":{},\"wall_ns\":{},\"logical_body_bytes\":{},\"encoded_bytes\":{},\"full_encodes\":{},\"delta_encodes\":{},\"cache_hits\":{},\"baseline_fallbacks\":{},\"expired_attempts\":{},\"missing_baselines\":{},\"applied\":{},\"lag_sum\":{},\"max_lag\":{},\"retained_revisions\":{},\"cached_bytes\":{},\"pending_attempts\":{},\"verified\":true}}",
        args[0],args[1],history,cache,peers,frames,case,sender_ns,start.elapsed().as_nanos(),logical_bytes,s.encoded_bytes,s.full_encodes,s.delta_encodes,s.cache_hits,s.baseline_fallbacks,s.expired_attempts,missing,applied,lag_sum,max_lag,sender.retained_revisions(),sender.cached_bytes(),sender.pending_attempts());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn both_codecs_exact_across_lengths_and_invalid_bytes() {
        for mask in [false, true] {
            let codec = Words { mask };
            for n in [0, 1, 63, 64, 65, 256] {
                let base: Vec<u64> = (0..n).collect();
                let mut state = base.clone();
                for (i, v) in state.iter_mut().enumerate() {
                    if i % 3 == 0 {
                        *v = 1000 + i as u64;
                    }
                }
                assert_eq!(
                    codec
                        .decode_full(&codec.encode_full(&state).unwrap())
                        .unwrap(),
                    state
                );
                assert_eq!(
                    codec
                        .decode_delta(&base, &codec.encode_delta(&base, &state).unwrap())
                        .unwrap(),
                    state
                );
                assert!(codec.decode_delta(&base, &[255; 9]).is_err());
            }
        }
    }
}
