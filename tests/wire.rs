mod support;
use rill_replicate::{
    wire::{self, Ack, Acks, Header, WireError},
    Config, Error, Receipt, Receiver, Sender, Session,
};
use support::{initial, MapCodec, STRATEGIES};

#[test]
fn all_integer_edges_and_borrowed_slice_identity() {
    for attempt in [1, 127, 128, 255, 16383, 16384, u64::MAX] {
        for revision in [0, 1, 127, 128, u64::MAX] {
            for baseline in [None, Some(0), Some(revision)] {
                let receipt = Receipt {
                    session: Session(55),
                    attempt,
                    revision,
                };
                let header = Header::new(receipt, baseline).unwrap();
                let body = [7; 32];
                let slices = header.slices(&body);
                assert_eq!(slices[1].as_ptr(), body.as_ptr());
                assert_eq!(&*slices[0], header.as_bytes());
                let mut message = Vec::new();
                header.append(&body, &mut message);
                let decoded = wire::decode(receipt.session, &message, 32).unwrap();
                assert_eq!(decoded.receipt, receipt);
                assert_eq!(decoded.baseline, baseline);
                assert_eq!(decoded.body, &body);
                assert_eq!(
                    decoded.body.as_ptr(),
                    message[header.as_bytes().len()..].as_ptr()
                );
                assert!(Ack::new(attempt).unwrap().as_bytes().len() <= 10);
                assert_eq!(
                    wire::decode_ack(Ack::new(attempt).unwrap().as_bytes()),
                    Ok(attempt)
                );
            }
        }
    }
}

#[test]
fn malformed_headers_and_ack_streams_fail_without_copying() {
    for bytes in [
        &[][..],
        &[0x10][..],
        &[0x12, 1, 0][..],
        &[0x10, 0, 0][..],
        &[0x10, 128, 0, 0][..],
        &[0x11, 1, 0, 1][..],
        &[0x10, 255, 255, 255, 255, 255, 255, 255, 255, 255, 2][..],
    ] {
        assert!(wire::decode(Session(1), bytes, 32).is_err());
    }
    assert!(Header::new(
        Receipt {
            session: Session(1),
            attempt: 0,
            revision: 0
        },
        None
    )
    .is_err());
    assert_eq!(
        wire::decode(Session(1), &[0x10, 1, 0, 5, 6], 1).unwrap_err(),
        WireError::PayloadLimit
    );
    let mut bytes = Vec::new();
    for n in [1, 128, u64::MAX] {
        bytes.extend_from_slice(Ack::new(n).unwrap().as_bytes());
    }
    assert_eq!(
        Acks::new(&bytes).collect::<Result<Vec<_>, _>>().unwrap(),
        [1, 128, u64::MAX]
    );
    assert_eq!(wire::decode_ack(&bytes), Err(WireError::TrailingBytes));
    let mut bad = Acks::new(&[128, 0, 1]);
    assert_eq!(bad.next(), Some(Err(WireError::InvalidInteger)));
    assert!(bad.next().is_none());
    assert!(Ack::new(0).is_err());
    assert!(wire::decode_ack(&[0]).is_err());
}

#[test]
fn every_policy_framed_borrowed_apply_exact_attempt_ack_and_session() {
    for strategy in STRATEGIES {
        let session = Session(9001);
        let mut sender = Sender::new(initial(16), MapCodec, Config::default(), strategy).unwrap();
        sender.add_peer(session).unwrap();
        let mut receiver = Receiver::new(session, 16).unwrap();
        for tick in 0..32 {
            let state = initial(16 + tick);
            if tick != 0 {
                sender.publish(state.clone()).unwrap();
            }
            let packet = sender.prepare(session).unwrap();
            let header = Header::for_envelope(&packet).unwrap();
            let mut message = Vec::new();
            header.append(&packet.body, &mut message);
            let frame = wire::decode(session, &message, 4096).unwrap();
            receiver
                .apply_parts(frame.receipt, frame.baseline, frame.body, &MapCodec)
                .unwrap();
            assert_eq!(receiver.current(), Some(&state));
            let ack =
                wire::decode_ack(Ack::new(frame.receipt.attempt).unwrap().as_bytes()).unwrap();
            assert!(!sender.acknowledge_attempt(Session(9002), ack));
            assert!(sender.acknowledge_attempt(session, ack));
            assert!(!sender.acknowledge_attempt(session, ack));
            assert!(!sender.acknowledge_attempt(session, u64::MAX));
            let foreign = wire::decode(Session(9002), &message, 4096).unwrap();
            assert_eq!(
                receiver.apply_parts(foreign.receipt, foreign.baseline, foreign.body, &MapCodec),
                Err(Error::WrongSession)
            );
        }
    }
}
