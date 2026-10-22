#[path = "support/byte_codecs.rs"]
mod byte_codecs;
use byte_codecs::{initial, mutate, Adapter};
use rill_replicate::Codec;
use std::{hint::black_box, time::Instant};

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 4, "codec case size seed");
    let codec = Adapter::new(&args[0]);
    let size: usize = args[2].parse().unwrap();
    let mut seed: u64 = args[3].parse().unwrap();
    let mut base = initial(size);
    let mut encode_ns = 0;
    let mut decode_ns = 0;
    let mut full_ns = 0;
    let mut delta_bytes = 0;
    let mut full_bytes = 0;
    let mut checksum = 0u64;
    for tick in 1..=32 {
        let mut state = base.clone();
        mutate(&mut state, &args[1], tick, &mut seed);
        let t = Instant::now();
        let full = codec.encode_full(black_box(&state)).unwrap();
        full_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        let delta = codec.delta(black_box(&base), black_box(&state)).unwrap();
        encode_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        let restored = codec.apply(black_box(&base), black_box(&delta)).unwrap();
        decode_ns += t.elapsed().as_nanos();
        assert_eq!(restored, state);
        assert_eq!(codec.decode_full(&full).unwrap(), state);
        full_bytes += full.len();
        delta_bytes += delta.len();
        checksum = state
            .iter()
            .fold(checksum, |s, v| s.wrapping_mul(31).wrapping_add(*v as u64));
        base = state;
    }
    println!("{{\"codec\":\"{}\",\"case\":\"{}\",\"size\":{},\"seed\":{},\"revisions\":32,\"encode_ns\":{},\"decode_ns\":{},\"full_encode_ns\":{},\"delta_bytes\":{},\"full_bytes\":{},\"checksum\":{},\"verified\":true}}",args[0],args[1],size,args[3],encode_ns,decode_ns,full_ns,delta_bytes,full_bytes,checksum);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_bidiff_empty_source_capability_requires_fallback() {
        for single in [false, true] {
            let result = std::panic::catch_unwind(|| {
                let mut out = Vec::new();
                if single {
                    bidiff::simple_diff_with_params(
                        &[],
                        &[1, 2],
                        &mut out,
                        &bidiff::DiffParams::new(1, None).unwrap(),
                    )
                } else {
                    bidiff::simple_diff(&[], &[1, 2], &mut out)
                }
            });
            assert!(
                result.is_err(),
                "Revisit full fallback if the pinned native implementation changes"
            );
            println!("native bidiff single={single}: empty-source panic reproduced; adapter uses explicit full fallback");
        }
    }
    #[test]
    fn every_codec_matches_variable_length_model() {
        let plan = include_str!("../research/byte-codec-plan.json");
        // Configured names are tested explicitly without a JSON dev dependency.
        let mut names = vec![
            "full".to_string(),
            "bsdiff".into(),
            "qbsdiff-default".into(),
            "qbsdiff-small".into(),
            "bidiff-default".into(),
            "bidiff-single".into(),
        ];
        for b in [1, 4, 8, 16, 64] {
            for g in [0, 4, 16, 64] {
                names.push(format!("runs-b{b}-g{g}"));
            }
        }
        names.extend(names.clone().into_iter().map(|n| format!("lz4+{n}")));
        assert_eq!(names.len(), 52);
        for name in names {
            assert!(plan.contains(&format!("\"{name}\"")));
            let codec = Adapter::new(&name);
            for case in ["sparse", "dense", "random", "shift", "resize"] {
                let mut seed = 4815162342;
                let mut state = initial(65);
                for tick in 0..32 {
                    let base = state.clone();
                    mutate(&mut state, case, tick, &mut seed);
                    let patch = codec.delta(&base, &state).unwrap();
                    assert_eq!(
                        codec.apply(&base, &patch).unwrap(),
                        state,
                        "{name}/{case}/{tick}"
                    );
                    let full = codec.encode_full(&state).unwrap();
                    assert_eq!(codec.decode_full(&full).unwrap(), state);
                }
                for (a, b) in [(vec![], vec![]), (vec![], vec![1, 2]), (vec![4, 5], vec![])] {
                    let patch = codec.delta(&a, &b).unwrap();
                    assert_eq!(codec.apply(&a, &patch).unwrap(), b, "{name}/empty");
                }
            }
        }
    }
}
