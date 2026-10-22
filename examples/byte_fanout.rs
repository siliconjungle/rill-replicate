//! Codec adapters inside Rill's actual sender/receiver, not standalone engines.
#[path = "support/byte_codecs.rs"]
mod byte_codecs;
use byte_codecs::{initial, mutate, Adapter};
use rill_replicate::{Applied, Config, Error, Receiver, Sender, Session, Strategy};
use std::time::Instant;

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 4, "codec policy peers trace");
    let strategy = match args[1].as_str() {
        "shared" => Strategy::SharedDelta,
        "adaptive" => Strategy::SharedAdaptive,
        _ => panic!("policy"),
    };
    let peers: usize = args[2].parse().unwrap();
    let loss = match args[3].as_str() {
        "sync" => false,
        "loss" => true,
        _ => panic!("trace"),
    };
    let config = Config {
        history: 16,
        peers,
        attempts_per_peer: 8,
        cache_entries: 64,
        cache_bytes: 1024 * 1024,
    };
    let codec = Adapter::new(&args[0]);
    let mut expected = initial(2048);
    let mut model = vec![expected.clone()];
    let mut sender =
        Sender::new(expected.clone(), Adapter::new(&args[0]), config, strategy).unwrap();
    let mut receivers: Vec<_> = (1..=peers)
        .map(|i| {
            sender.add_peer(Session(i as u128)).unwrap();
            Receiver::new(Session(i as u128), 16).unwrap()
        })
        .collect();
    let mut pending = Vec::new();
    let mut seed = 4815162342;
    let mut sender_ns = 0;
    let mut transmitted = 0usize;
    let mut applied = 0;
    let mut missing = 0;
    let start = Instant::now();
    for tick in 0..34 {
        if tick > 0 && tick < 32 {
            mutate(&mut expected, "sparse", tick, &mut seed);
            model.push(expected.clone());
            let t = Instant::now();
            sender.publish(expected.clone()).unwrap();
            sender_ns += t.elapsed().as_nanos();
        }
        let mut outbound = Vec::new();
        let t = Instant::now();
        if tick == 32 {
            for id in 1..=peers {
                sender.resynchronize(Session(id as u128)).unwrap();
            }
        }
        for i in 0..peers {
            if loss && tick > 0 && tick < 32 && i % 97 == 0 {
                continue;
            }
            outbound.push((i, sender.prepare(Session(i as u128 + 1)).unwrap()));
        }
        sender_ns += t.elapsed().as_nanos();
        let mut incoming = Vec::new();
        for (i, packet) in outbound {
            transmitted += packet.body.len();
            if loss && tick < 32 {
                if (i + tick) % 11 == 0 {
                    continue;
                }
                if (i + tick) % 13 == 0 {
                    pending.push((i, packet));
                    continue;
                }
                if (i + tick) % 17 == 0 {
                    transmitted += packet.body.len();
                    incoming.push((i, packet.clone()));
                }
            }
            incoming.push((i, packet));
        }
        if tick % 3 == 0 || tick >= 32 {
            incoming.append(&mut pending);
        }
        incoming.reverse();
        for (i, packet) in incoming {
            match receivers[i].apply(&packet, &codec) {
                Ok(Applied::Updated) => {
                    assert_eq!(
                        receivers[i].current(),
                        Some(&model[packet.receipt.revision as usize])
                    );
                    applied += 1;
                    if !loss || tick >= 32 || (i + tick) % 5 != 0 {
                        let t = Instant::now();
                        sender.acknowledge(packet.receipt);
                        sender_ns += t.elapsed().as_nanos();
                    }
                }
                Ok(Applied::Stale) => {
                    if receivers[i].revision() == Some(packet.receipt.revision) {
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
        assert!(sender.retained_revisions() <= 16);
        assert!(sender.pending_attempts() <= peers * 8);
        assert!(sender.cached_bytes() <= config.cache_bytes);
        assert!(receivers.iter().all(|r| r.retained_revisions() <= 16));
    }
    for (i, receiver) in receivers.iter().enumerate() {
        assert_eq!(receiver.current(), Some(&expected));
        assert_eq!(sender.acknowledged(Session(i as u128 + 1)), Some(31));
    }
    let s = sender.stats();
    println!("{{\"codec\":\"{}\",\"policy\":\"{}\",\"peers\":{},\"trace\":\"{}\",\"sender_ns\":{},\"wall_ns\":{},\"transmitted_body_bytes\":{},\"encoded_bytes\":{},\"full_encodes\":{},\"delta_encodes\":{},\"cache_hits\":{},\"missing_baselines\":{},\"applied\":{},\"verified\":true}}",args[0],args[1],peers,args[3],sender_ns,start.elapsed().as_nanos(),transmitted,s.encoded_bytes,s.full_encodes,s.delta_encodes,s.cache_hits,missing,applied);
}
