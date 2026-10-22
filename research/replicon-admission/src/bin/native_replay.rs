//! All native server peers alive together; every recipient is decoded separately.
//! Predicted ACKs are accepted only after exhaustive exact native decoder replay.
#[path = "../common.rs"]
mod common;
use bevy::{prelude::*, state::app::StatesPlugin, time::TimeUpdateStrategy};
use bevy_replicon::{
    bytes::Bytes,
    postcard,
    prelude::*,
    shared::backend::channels::{ClientChannel, ServerChannel},
};
use common::{Change, Dataset};
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

fn app(diff: bool, server: bool) -> App {
    let mut app = App::new();
    let plugins = RepliconPlugins.set(ServerPlugin::new(PostUpdate));
    let plugins = if server {
        plugins
            .disable::<ClientPlugin>()
            .disable::<ClientMessagePlugin>()
    } else {
        plugins
            .disable::<ServerPlugin>()
            .disable::<ServerMessagePlugin>()
    };
    app.add_plugins((MinimalPlugins, StatesPlugin, plugins));
    if diff {
        app.replicate_diff::<Dataset>();
    } else {
        app.replicate::<Dataset>();
    }
    app.insert_resource(TimeUpdateStrategy::ManualDuration(Duration::from_nanos(
        16_666_667,
    )));
    app.finish();
    app
}
fn connected_client(diff: bool) -> App {
    let mut client = app(diff, false);
    client
        .world_mut()
        .resource_mut::<NextState<ClientState>>()
        .set(ClientState::Connected);
    client
}
type Message = (usize, Bytes);
#[derive(Default, Clone)]
struct Frame {
    incoming: Vec<Message>,
    outgoing: Vec<Message>,
    required_applied: Option<u64>,
}
struct Trace {
    prefix: Vec<Vec<Message>>,
    frames: Vec<Frame>,
}
struct HeldMutation {
    due: usize,
    peer: usize,
    message: Message,
    target: u64,
}
struct HeldAck {
    due: usize,
    peer: usize,
    message: Message,
    target: u64,
}
fn maximum(slot: &mut Option<u64>, value: u64) {
    *slot = Some(slot.map_or(value, |old| old.max(value)));
}
fn output(client: &mut App) -> Vec<Message> {
    client
        .world_mut()
        .resource_mut::<ClientMessages>()
        .drain_sent()
        .collect()
}
fn insert(client: &mut App, messages: &[Message]) {
    let mut incoming = client.world_mut().resource_mut::<ClientMessages>();
    for (channel, body) in messages {
        incoming.insert_received(*channel, body.clone());
    }
}
fn predict(messages: &[Message]) -> Option<Message> {
    let mut acks = Vec::new();
    for (channel, body) in messages {
        if *channel == usize::from(ServerChannel::Mutations) {
            // Pinned native format: postcard u8 flags, then fixed LE u16 index.
            // Native replay must produce this exact byte stream for every peer.
            let (_, rest) = postcard::take_from_bytes::<u8>(body).expect("native flags");
            acks.extend_from_slice(rest.get(..2).expect("native mutation index"));
        }
    }
    (!acks.is_empty()).then(|| (usize::from(ClientChannel::MutationAcks), Bytes::from(acks)))
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 4, "full|diff peers sync|loss ack-delay");
    let diff = match args[0].as_str() {
        "full" => false,
        "diff" => true,
        _ => panic!("mode"),
    };
    let peers: usize = args[1].parse().unwrap();
    assert!(peers > 0 && peers <= 20000);
    let loss = match args[2].as_str() {
        "sync" => false,
        "loss" => true,
        _ => panic!("trace"),
    };
    let delay: usize = args[3].parse().unwrap();
    assert!(delay <= 64);
    let ticks = 33 + delay + 8;
    let setup = Instant::now();
    let mut prototype = connected_client(diff);
    let mut prefix_out = Vec::new();
    for _ in 0..4 {
        prototype.update();
        prefix_out.push(output(&mut prototype));
    }
    drop(prototype);
    let mut server = app(diff, true);
    server
        .world_mut()
        .resource_mut::<NextState<ServerState>>()
        .set(ServerState::Running);
    let entities: Vec<_> = (0..peers)
        .map(|_| {
            server
                .world_mut()
                .spawn(ConnectedClient { max_size: 1200 })
                .id()
        })
        .collect();
    let index: HashMap<_, _> = entities.iter().enumerate().map(|(i, e)| (*e, i)).collect();
    let mut traces: Vec<_> = (0..peers)
        .map(|_| Trace {
            prefix: vec![Vec::new(); 4],
            frames: vec![Frame::default(); ticks],
        })
        .collect();
    for (step, messages) in prefix_out.iter().enumerate() {
        for entity in &entities {
            let mut input = server.world_mut().resource_mut::<ServerMessages>();
            for (channel, body) in messages {
                input.insert_received(*entity, *channel, body.clone());
            }
        }
        server.update();
        for (entity, channel, body) in server
            .world_mut()
            .resource_mut::<ServerMessages>()
            .drain_sent()
        {
            assert_ne!(
                channel,
                usize::from(ServerChannel::Mutations),
                "handshake must precede dataset publication"
            );
            traces[index[&entity]].prefix[step].push((channel, body));
        }
    }
    assert!(
        entities
            .iter()
            .all(|e| server.world().get::<AuthorizedClient>(*e).is_some())
    );
    let setup_ns = setup.elapsed().as_nanos();
    let mut expected = Dataset {
        revision: 0,
        bytes: (0..2048).map(|i| (i / 8) as u8).collect(),
    };
    let mut models = vec![expected.clone()];
    let entity = server
        .world_mut()
        .spawn((Replicated, expected.clone()))
        .id();
    let mut held_mutations = VecDeque::<HeldMutation>::new();
    let mut held_acks = VecDeque::<HeldAck>::new();
    let mut server_ns = 0;
    let mut mutation_ns = 0;
    let mut export_ns = 0;
    let mut oracle_ns = 0;
    let mut wire_bytes = 0usize;
    let mut ack_bytes = 0usize;
    let mut dropped = 0;
    let mut duplicated = 0;
    let mut delayed = 0;
    let mut replay_targets = vec![None; peers];
    let generate = Instant::now();
    for tick in 0..ticks {
        if tick > 0 && tick <= 32 {
            let change = Change {
                revision: tick as u64,
                edits: vec![(tick * 13 % expected.bytes.len(), tick as u8)],
            };
            expected.apply_diff(&change).unwrap();
            models.push(expected.clone());
            let t = Instant::now();
            if diff {
                server
                    .world_mut()
                    .entity_mut(entity)
                    .apply_diff::<Dataset>(change)
                    .unwrap();
            } else {
                *server.world_mut().get_mut::<Dataset>(entity).unwrap() = expected.clone();
            }
            mutation_ns += t.elapsed().as_nanos();
        }
        let t = Instant::now();
        server.update();
        server_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        replay_targets.fill(None);
        for (peer_entity, channel, body) in server
            .world_mut()
            .resource_mut::<ServerMessages>()
            .drain_sent()
        {
            let peer = index[&peer_entity];
            wire_bytes += body.len();
            let target = expected.revision;
            if loss && tick <= 32 && channel == usize::from(ServerChannel::Mutations) {
                if (tick + peer) % 11 == 0 {
                    dropped += 1;
                    continue;
                }
                if (tick + peer) % 13 == 0 {
                    held_mutations.push_back(HeldMutation {
                        due: tick + 3,
                        peer,
                        message: (channel, body),
                        target,
                    });
                    delayed += 1;
                    continue;
                }
                if (tick + peer) % 17 == 0 {
                    wire_bytes += body.len();
                    traces[peer].frames[tick]
                        .incoming
                        .push((channel, body.clone()));
                    duplicated += 1;
                }
            }
            if channel == usize::from(ServerChannel::Mutations) {
                maximum(&mut replay_targets[peer], target);
            }
            traces[peer].frames[tick].incoming.push((channel, body));
        }
        while held_mutations.front().is_some_and(|p| p.due <= tick) {
            let packet = held_mutations.pop_front().unwrap();
            maximum(&mut replay_targets[packet.peer], packet.target);
            traces[packet.peer].frames[tick]
                .incoming
                .push(packet.message);
        }
        export_ns += t.elapsed().as_nanos();
        let t = Instant::now();
        for (peer, trace) in traces.iter_mut().enumerate() {
            if let Some(message) = predict(&trace.frames[tick].incoming) {
                ack_bytes += message.1.len();
                trace.frames[tick].outgoing.push(message.clone());
                held_acks.push_back(HeldAck {
                    due: tick + delay,
                    peer,
                    message,
                    target: replay_targets[peer].unwrap(),
                });
            }
        }
        while held_acks.front().is_some_and(|p| p.due <= tick) {
            let packet = held_acks.pop_front().unwrap();
            maximum(
                &mut traces[packet.peer].frames[tick].required_applied,
                packet.target,
            );
            server
                .world_mut()
                .resource_mut::<ServerMessages>()
                .insert_received(entities[packet.peer], packet.message.0, packet.message.1);
        }
        oracle_ns += t.elapsed().as_nanos();
    }
    assert!(held_mutations.is_empty());
    let generation_ns = generate.elapsed().as_nanos();
    let trace_messages: usize = traces
        .iter()
        .map(|p| p.frames.iter().map(|f| f.incoming.len()).sum::<usize>())
        .sum();
    // No success is emitted until EVERY independent native decoder reproduces
    // all predicted replies and all complete values at every logical step.
    let replay = Instant::now();
    let mut full_checks = 0usize;
    let mut exact_ack_frames = 0usize;
    for (peer, trace) in traces.iter().enumerate() {
        let mut client = connected_client(diff);
        for (step, expected_out) in prefix_out.iter().enumerate() {
            client.update();
            assert_eq!(
                output(&mut client),
                *expected_out,
                "handshake peer {peer} step {step}"
            );
            insert(&mut client, &trace.prefix[step]);
        }
        for (tick, frame) in trace.frames.iter().enumerate() {
            insert(&mut client, &frame.incoming);
            client.update();
            assert_eq!(
                output(&mut client),
                frame.outgoing,
                "ACK peer {peer} tick {tick}"
            );
            exact_ack_frames += 1;
            let mut query = client.world_mut().query::<&Dataset>();
            let value = query.single(client.world()).unwrap();
            assert!(value.revision <= 32);
            assert_eq!(*value, models[value.revision as usize]);
            full_checks += 1;
            if let Some(target) = frame.required_applied {
                assert!(
                    value.revision >= target,
                    "ACK preceded application peer={peer} tick={tick} target={target} actual={}",
                    value.revision
                );
            }
            if tick == ticks - 1 {
                assert_eq!(*value, expected);
            }
        }
    }
    if loss && peers >= 8 {
        assert!(dropped > 0 && duplicated > 0 && delayed > 0);
    }
    println!(
        "{{\"mode\":\"{}\",\"server_peers\":{},\"native_decoder_replays\":{},\"trace\":\"{}\",\"ack_delay\":{},\"frames\":32,\"fixed_clock_ns\":16666667,\"setup_ns\":{},\"server_update_ns\":{},\"mutation_ns\":{},\"export_trace_ns\":{},\"ack_oracle_ns\":{},\"generation_ns\":{},\"replay_verification_ns\":{},\"server_wire_bytes\":{},\"ack_wire_bytes\":{},\"trace_messages\":{},\"dropped\":{},\"duplicated\":{},\"delayed\":{},\"full_value_checks\":{},\"exact_ack_frames\":{},\"verified\":true,\"timing_acceptance\":false}}",
        args[0],
        peers,
        peers,
        args[2],
        delay,
        setup_ns,
        server_ns,
        mutation_ns,
        export_ns,
        oracle_ns,
        generation_ns,
        replay.elapsed().as_nanos(),
        wire_bytes,
        ack_bytes,
        trace_messages,
        dropped,
        duplicated,
        delayed,
        full_checks,
        exact_ack_frames
    );
}
