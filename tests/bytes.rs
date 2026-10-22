use rill_replicate::{
    bytes::{ByteError, ByteRuns},
    Codec,
};

fn rand(s: &mut u64) -> usize {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s as usize
}

#[test]
fn all_run_configs_exact_variable_size_model() {
    for block in [1, 4, 8, 16, 64] {
        for gap in [0, 4, 16, 64] {
            let codec = ByteRuns::new(block, gap, 4096).unwrap();
            let mut seed = 4815162342;
            let mut base = Vec::new();
            for _ in 0..1000 {
                let mut state = base.clone();
                match rand(&mut seed) % 4 {
                    0 => state.resize(rand(&mut seed) % 513, 0),
                    1 => {
                        state = (0..rand(&mut seed) % 513)
                            .map(|_| rand(&mut seed) as u8)
                            .collect()
                    }
                    _ => {
                        if !state.is_empty() {
                            let at = rand(&mut seed) % state.len();
                            state[at] = rand(&mut seed) as u8;
                        }
                    }
                }
                let delta = codec.diff(&base, &state).unwrap();
                assert_eq!(codec.patch(&base, &delta).unwrap(), state);
                assert_eq!(
                    codec
                        .decode_full(&codec.encode_full(&state).unwrap())
                        .unwrap(),
                    state
                );
                base = state;
            }
        }
    }
}

#[test]
fn malformed_patch_and_output_limits() {
    assert!(ByteRuns::new(0, 0, 16).is_none());
    let codec = ByteRuns::new(1, 0, 16).unwrap();
    assert_eq!(codec.diff(&[], &[0; 17]), Err(ByteError::Limit));
    assert_eq!(codec.decode_full(&[0; 17]), Err(ByteError::Limit));
    let mut header = b"RR01".to_vec();
    header.extend_from_slice(&17u64.to_le_bytes());
    assert_eq!(codec.patch(&[], &header), Err(ByteError::Limit));
    header[4..12].copy_from_slice(&8u64.to_le_bytes());
    for tail in [
        vec![0, 0],
        vec![9, 1, 3],
        vec![0, 17],
        vec![128, 0, 1, 3],
        vec![255; 12],
    ] {
        let mut bytes = header.clone();
        bytes.extend(tail);
        assert_eq!(codec.patch(&[], &bytes), Err(ByteError::Malformed));
    }
    let original = codec.diff(&[0; 16], &[9; 16]).unwrap();
    for i in 0..original.len() {
        let result = codec.patch(&[0; 16], &original[..i]);
        if let Ok(state) = result {
            assert!(state.len() <= 16);
        }
    }
}
