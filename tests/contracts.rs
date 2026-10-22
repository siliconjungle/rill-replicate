mod support;
use rill_replicate::{Applied, Codec, Config, Error, Receiver, Sender, Session, Strategy};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use support::{initial, MapCodec, State, STRATEGIES};

#[test]
fn exact_attempt_ack_session_and_monotonic_baseline() {
    for strategy in STRATEGIES {
        let mut sender = Sender::new(initial(8), MapCodec, Config::default(), strategy).unwrap();
        let session = Session(1);
        sender.add_peer(session).unwrap();
        let a = sender.prepare(session).unwrap();
        assert_eq!(sender.acknowledged(session), None);
        let mut forged = a.receipt;
        forged.revision += 1;
        assert!(!sender.acknowledge(forged));
        let mut receiver = Receiver::new(session, 4).unwrap();
        assert_eq!(receiver.apply(&a, &MapCodec), Ok(Applied::Updated));
        sender.publish(initial(9)).unwrap();
        let b = sender.prepare(session).unwrap();
        receiver.apply(&b, &MapCodec).unwrap();
        assert!(sender.acknowledge(b.receipt));
        assert!(sender.acknowledge(a.receipt));
        assert_eq!(sender.acknowledged(session), Some(1));
        assert!(!sender.acknowledge(b.receipt));
        sender.remove_peer(session);
        sender.add_peer(Session(2)).unwrap();
        assert!(!sender.acknowledge(a.receipt));
        assert_eq!(
            Receiver::<State>::new(Session(2), 4)
                .unwrap()
                .apply(&a, &MapCodec),
            Err(Error::WrongSession)
        );
    }
}

#[test]
fn missing_baseline_decode_failure_resync_and_late_packets() {
    for strategy in STRATEGIES
        .into_iter()
        .filter(|s| !matches!(s, Strategy::Full | Strategy::SharedFull))
    {
        let session = Session(9);
        let mut sender = Sender::new(initial(32), MapCodec, Config::default(), strategy).unwrap();
        sender.add_peer(session).unwrap();
        let mut receiver = Receiver::new(session, 1).unwrap();
        let first = sender.prepare(session).unwrap();
        receiver.apply(&first, &MapCodec).unwrap();
        sender.acknowledge(first.receipt);
        let mut state = initial(32);
        state.insert(1, 19);
        sender.publish(state.clone()).unwrap();
        let a = sender.prepare(session).unwrap();
        receiver.apply(&a, &MapCodec).unwrap(); // ACK lost
        state.insert(2, 97);
        sender.publish(state.clone()).unwrap();
        let b = sender.prepare(session).unwrap();
        assert_eq!(b.baseline, Some(0));
        assert_eq!(
            receiver.apply(&b, &MapCodec),
            Err(Error::MissingBaseline(0))
        );
        assert_eq!(receiver.revision(), Some(1));
        sender.resynchronize(session).unwrap();
        assert!(!sender.acknowledge(a.receipt));
        let full = sender.prepare(session).unwrap();
        assert_eq!(full.baseline, None);
        let mut corrupt = full.clone();
        corrupt.body = Arc::from(&b"bad"[..]);
        assert!(matches!(
            receiver.apply(&corrupt, &MapCodec),
            Err(Error::Codec(_))
        ));
        assert_eq!(receiver.revision(), Some(1));
        receiver.apply(&full, &MapCodec).unwrap();
        sender.acknowledge(full.receipt);
        assert_eq!(receiver.current(), Some(&state));
        assert_eq!(receiver.apply(&a, &MapCodec), Ok(Applied::Stale));
        assert_eq!(receiver.current(), Some(&state));
    }
}

#[test]
fn sharing_fallback_and_bounds() {
    for strategy in STRATEGIES {
        let cfg = Config {
            history: 2,
            peers: 2,
            attempts_per_peer: 2,
            cache_entries: 2,
            cache_bytes: 4096,
        };
        let mut sender = Sender::new(initial(8), MapCodec, cfg, strategy).unwrap();
        for id in 1..=2 {
            sender.add_peer(Session(id)).unwrap();
        }
        assert_eq!(sender.add_peer(Session(1)), Err(Error::DuplicateSession));
        assert_eq!(sender.add_peer(Session(3)), Err(Error::PeerLimit));
        let a = sender.prepare(Session(1)).unwrap();
        let b = sender.prepare(Session(2)).unwrap();
        assert_eq!(
            Arc::ptr_eq(&a.body, &b.body),
            matches!(
                strategy,
                Strategy::SharedFull | Strategy::SharedDelta | Strategy::SharedAdaptive
            )
        );
        assert!(sender.acknowledge(a.receipt));
        sender.publish(initial(9)).unwrap();
        sender.publish(initial(10)).unwrap();
        let c = sender.prepare(Session(1)).unwrap();
        assert_eq!(c.baseline, None);
        let mut last = c.receipt;
        for _ in 0..10 {
            last = sender.prepare(Session(1)).unwrap().receipt;
        }
        assert!(!sender.acknowledge(c.receipt));
        assert!(sender.acknowledge(last));
        assert!(sender.retained_revisions() <= 2);
        assert!(sender.pending_attempts() <= 4);
        assert!(sender.cached_bytes() <= cfg.cache_bytes);
        assert!(sender.cached_entries() <= cfg.cache_entries);
    }
}

#[test]
fn deterministic_loss_reordering_delete_reinsert_model_all_policies() {
    for strategy in STRATEGIES {
        for history in [1, 4, 16] {
            for entries in [0, 64] {
                let cfg = Config {
                    history,
                    peers: 24,
                    attempts_per_peer: 4,
                    cache_entries: entries,
                    cache_bytes: 4096,
                };
                let mut expected = initial(24);
                let mut sender = Sender::new(expected.clone(), MapCodec, cfg, strategy).unwrap();
                let mut receivers: Vec<_> = (1..=24)
                    .map(|id| {
                        sender.add_peer(Session(id)).unwrap();
                        Receiver::new(Session(id), history).unwrap()
                    })
                    .collect();
                let mut snapshots = vec![expected.clone()];
                let mut delayed = Vec::new();
                for tick in 0..80usize {
                    if tick != 0 {
                        expected.insert(tick as u64 % 31, tick as u64 * 99);
                        expected.remove(&((tick as u64 + 11) % 31));
                        sender.publish(expected.clone()).unwrap();
                        snapshots.push(expected.clone());
                    }
                    for peer in 0..24 {
                        let packet = sender.prepare(Session(peer as u128 + 1)).unwrap();
                        if (peer + tick) % 7 == 0 {
                            continue;
                        }
                        delayed.push((peer, packet));
                    }
                    delayed.reverse();
                    let mut next = Vec::new();
                    for (i, packet) in delayed.drain(..) {
                        if tick % 3 == 0 && i % 4 == 0 {
                            next.push((i, packet));
                            continue;
                        }
                        match receivers[i].apply(&packet, &MapCodec) {
                            Ok(Applied::Updated) => {
                                assert_eq!(
                                    receivers[i].current(),
                                    Some(&snapshots[packet.receipt.revision as usize])
                                );
                                if (i + tick) % 5 != 0 {
                                    sender.acknowledge(packet.receipt);
                                }
                                assert_eq!(
                                    receivers[i].apply(&packet, &MapCodec),
                                    Ok(Applied::Stale)
                                );
                            }
                            Ok(Applied::Stale) => (),
                            Err(Error::MissingBaseline(_)) => {
                                sender.resynchronize(packet.receipt.session).unwrap()
                            }
                            e => panic!("unexpected {e:?}"),
                        }
                    }
                    delayed = next;
                    assert!(sender.retained_revisions() <= history);
                    assert!(sender.pending_attempts() <= 96);
                    assert!(sender.cached_bytes() <= cfg.cache_bytes);
                }
                for (i, receiver) in receivers.iter_mut().enumerate() {
                    let session = Session(i as u128 + 1);
                    sender.resynchronize(session).unwrap();
                    let packet = sender.prepare(session).unwrap();
                    receiver.apply(&packet, &MapCodec).unwrap();
                    assert_eq!(receiver.current(), Some(&expected));
                    sender.acknowledge(packet.receipt);
                    assert_eq!(sender.acknowledged(session), Some(sender.revision()));
                }
            }
        }
    }
}

#[test]
fn dense_adaptive_chooses_full_and_sparse_shares_delta() {
    for strategy in [Strategy::SharedAdaptive, Strategy::SharedDelta] {
        let mut sender = Sender::new(initial(32), MapCodec, Config::default(), strategy).unwrap();
        for id in [1, 2] {
            sender.add_peer(Session(id)).unwrap();
            let first = sender.prepare(Session(id)).unwrap();
            sender.acknowledge(first.receipt);
        }
        let state: State = (0..32).map(|k| (k, 777)).collect();
        sender.publish(state).unwrap();
        let first = sender.prepare(Session(1)).unwrap();
        let second = sender.prepare(Session(2)).unwrap();
        assert_eq!(
            first.baseline.is_none(),
            strategy == Strategy::SharedAdaptive
        );
        assert!(Arc::ptr_eq(&first.body, &second.body));
    }
}

struct Counted(Arc<AtomicUsize>);
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct CountCodec;
impl Codec<Counted> for CountCodec {
    type Error = ();
    fn encode_full(&self, _: &Counted) -> Result<Vec<u8>, ()> {
        Ok(vec![1])
    }
    fn encode_delta(&self, _: &Counted, _: &Counted) -> Result<Vec<u8>, ()> {
        Ok(vec![1])
    }
    fn decode_full(&self, _: &[u8]) -> Result<Counted, ()> {
        Err(())
    }
    fn decode_delta(&self, _: &Counted, _: &[u8]) -> Result<Counted, ()> {
        Err(())
    }
}

#[test]
fn stalled_peer_does_not_pin_application_history() {
    let live = Arc::new(AtomicUsize::new(1));
    let mut sender = Sender::new(
        Counted(live.clone()),
        CountCodec,
        Config {
            history: 2,
            ..Config::default()
        },
        Strategy::SharedDelta,
    )
    .unwrap();
    sender.add_peer(Session(1)).unwrap();
    let first = sender.prepare(Session(1)).unwrap();
    sender.acknowledge(first.receipt);
    for _ in 0..100 {
        live.fetch_add(1, Ordering::SeqCst);
        sender.publish(Counted(live.clone())).unwrap();
        assert!(live.load(Ordering::SeqCst) <= 2);
    }
    drop(sender);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn malformed_codec_inputs_do_not_allocate_from_untrusted_counts() {
    for bytes in [&[][..], &[255; 8][..], &[0; 9][..]] {
        assert!(MapCodec.decode_full(bytes).is_err());
        assert!(MapCodec.decode_delta(&initial(2), bytes).is_err());
    }
}

struct Fails(std::cell::Cell<bool>);
impl Codec<State> for Fails {
    type Error = &'static str;
    fn encode_full(&self, state: &State) -> Result<Vec<u8>, Self::Error> {
        if self.0.replace(false) {
            Err("injected")
        } else {
            MapCodec.encode_full(state)
        }
    }
    fn encode_delta(&self, base: &State, state: &State) -> Result<Vec<u8>, Self::Error> {
        if self.0.replace(false) {
            Err("injected")
        } else {
            MapCodec.encode_delta(base, state)
        }
    }
    fn decode_full(&self, b: &[u8]) -> Result<State, Self::Error> {
        MapCodec.decode_full(b)
    }
    fn decode_delta(&self, base: &State, b: &[u8]) -> Result<State, Self::Error> {
        MapCodec.decode_delta(base, b)
    }
}

#[test]
fn encoding_failure_does_not_create_an_attempt() {
    for strategy in STRATEGIES {
        let mut sender = Sender::new(
            initial(8),
            Fails(std::cell::Cell::new(true)),
            Config::default(),
            strategy,
        )
        .unwrap();
        sender.add_peer(Session(1)).unwrap();
        assert!(matches!(
            sender.prepare(Session(1)),
            Err(Error::Codec("injected"))
        ));
        assert_eq!(sender.pending_attempts(), 0);
        let envelope = sender.prepare(Session(1)).unwrap();
        assert_eq!(envelope.receipt.attempt, 1);
        assert_eq!(sender.acknowledged(Session(1)), None);
    }
}

#[test]
fn cached_bodies_reclaim_and_invalid_envelopes_preserve_receiver() {
    let mut sender = Sender::new(
        initial(8),
        MapCodec,
        Config::default(),
        Strategy::SharedDelta,
    )
    .unwrap();
    sender.add_peer(Session(1)).unwrap();
    let envelope = sender.prepare(Session(1)).unwrap();
    let weak = Arc::downgrade(&envelope.body);
    let mut receiver = Receiver::new(Session(1), 2).unwrap();
    let mut future = envelope.clone();
    future.baseline = Some(1);
    assert_eq!(
        receiver.apply(&future, &MapCodec),
        Err(Error::InvalidEnvelope)
    );
    assert_eq!(receiver.current(), None);
    drop(future);
    drop(envelope);
    assert!(weak.upgrade().is_some());
    sender.publish(initial(9)).unwrap();
    assert!(weak.upgrade().is_none());
    assert!(Receiver::<State>::new(Session(1), 0).is_none());
    assert!(matches!(
        Sender::new(
            initial(1),
            MapCodec,
            Config {
                history: 0,
                ..Config::default()
            },
            Strategy::Full
        ),
        Err(Error::InvalidConfig)
    ));
}
