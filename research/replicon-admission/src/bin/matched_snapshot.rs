//! Same snapshot input and complete-message boundary, exhaustive decoder replay.
#[path = "../../../../examples/support/byte_codecs.rs"]
mod byte_codecs;
#[path = "../common.rs"]
#[allow(dead_code)] // Diffable fixture is shared with the separate authored-edit proof.
mod common;
use bevy::{prelude::*, state::app::StatesPlugin, time::TimeUpdateStrategy};
use bevy_replicon::{
    bytes::Bytes,
    postcard,
    prelude::*,
    shared::backend::channels::{ClientChannel, ServerChannel},
};
use byte_codecs::Adapter;
use common::Dataset;
use rill_replicate::{Config, Receiver, Sender, Session, Strategy, wire};
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

type Message = (usize, Bytes);
type Packet = (usize, Message);
const CLOCK: u64 = 16_666_667;
const HISTORY: usize = 16;
fn session(peer: usize) -> Session {
    Session(peer as u128 + 1)
}
fn native_app(server: bool) -> App {
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
    app.replicate::<Dataset>();
    app.insert_resource(TimeUpdateStrategy::ManualDuration(Duration::from_nanos(
        CLOCK,
    )));
    app.finish();
    app
}
fn native_client() -> App {
    let mut app = native_app(false);
    app.world_mut()
        .resource_mut::<NextState<ClientState>>()
        .set(ClientState::Connected);
    app
}
fn native_output(app: &mut App) -> Vec<Message> {
    app.world_mut()
        .resource_mut::<ClientMessages>()
        .drain_sent()
        .collect()
}
fn strategy(name: &str) -> Strategy {
    match name {
        "full" => Strategy::Full,
        "shared-full" => Strategy::SharedFull,
        "individual" => Strategy::PerRecipientDelta,
        "shared" => Strategy::SharedDelta,
        "adaptive" => Strategy::SharedAdaptive,
        _ => panic!("unknown strategy"),
    }
}
fn serialize(value: &Dataset) -> Vec<u8> {
    postcard::to_extend(value, Vec::new()).unwrap()
}

enum Engine {
    Native {
        app: Box<App>,
        entities: Vec<Entity>,
        index: HashMap<Entity, usize>,
        dataset: Entity,
    },
    Rill {
        sender: Sender<Vec<u8>, Adapter>,
        peers: usize,
        inbox: Vec<Packet>,
        initial: bool,
    },
}
impl Engine {
    fn publish(&mut self, value: &Dataset) {
        match self {
            Self::Native { app, dataset, .. } => {
                *app.world_mut().get_mut::<Dataset>(*dataset).unwrap() = value.clone();
            }
            Self::Rill { sender, .. } => {
                sender.publish(serialize(value)).unwrap();
            }
        }
    }
    fn step(&mut self) -> Vec<Packet> {
        match self {
            Self::Native { app, index, .. } => {
                app.update();
                app.world_mut()
                    .resource_mut::<ServerMessages>()
                    .drain_sent()
                    .map(|(entity, channel, body)| (index[&entity], (channel, body)))
                    .collect()
            }
            Self::Rill {
                sender,
                peers,
                inbox,
                initial,
            } => {
                for (peer, (_, bytes)) in inbox.drain(..) {
                    for attempt in wire::Acks::new(&bytes) {
                        // Expired/duplicate attempts are legitimate and never confirm data.
                        sender.acknowledge_attempt(session(peer), attempt.unwrap());
                    }
                }
                let mut output = Vec::new();
                for peer in 0..*peers {
                    if sender.acknowledged(session(peer)) == Some(sender.revision()) {
                        continue;
                    }
                    let envelope = sender.prepare(session(peer)).unwrap();
                    let header = wire::Header::for_envelope(&envelope).unwrap();
                    let mut bytes =
                        Vec::with_capacity(header.as_bytes().len() + envelope.body.len());
                    header.append(&envelope.body, &mut bytes);
                    let channel = if *initial {
                        ServerChannel::Updates
                    } else {
                        ServerChannel::Mutations
                    };
                    output.push((peer, (usize::from(channel), Bytes::from(bytes))));
                }
                *initial = false;
                output
            }
        }
    }
    fn ingest(&mut self, peer: usize, message: Message) {
        match self {
            Self::Native { app, entities, .. } => {
                app.world_mut()
                    .resource_mut::<ServerMessages>()
                    .insert_received(entities[peer], message.0, message.1);
            }
            Self::Rill { inbox, .. } => inbox.push((peer, message)),
        }
    }
}

enum Client {
    Native(Box<App>),
    Rill {
        receiver: Receiver<Vec<u8>>,
        codec: Adapter,
        peer: usize,
        inbox: Vec<Message>,
        current: Option<Dataset>,
    },
}
impl Client {
    fn new(native: bool, codec: &str, peer: usize) -> Self {
        if native {
            Self::Native(Box::new(native_client()))
        } else {
            Self::Rill {
                receiver: Receiver::new(session(peer), HISTORY).unwrap(),
                codec: Adapter::new(codec),
                peer,
                inbox: Vec::new(),
                current: None,
            }
        }
    }
    fn ingest(&mut self, messages: &[Message]) {
        match self {
            Self::Native(app) => {
                let mut input = app.world_mut().resource_mut::<ClientMessages>();
                for (channel, body) in messages {
                    input.insert_received(*channel, body.clone());
                }
            }
            Self::Rill { inbox, .. } => inbox.extend_from_slice(messages),
        }
    }
    fn step(&mut self) -> Vec<Message> {
        match self {
            Self::Native(app) => {
                app.update();
                native_output(app)
            }
            Self::Rill {
                receiver,
                codec,
                peer,
                inbox,
                current,
            } => {
                let mut acks = Vec::new();
                for (_, body) in inbox.drain(..) {
                    let decoded =
                        wire::decode(session(*peer), &body, byte_codecs::LIMIT * 4).unwrap();
                    let applied = receiver
                        .apply_parts(decoded.receipt, decoded.baseline, decoded.body, codec)
                        .unwrap();
                    if applied == rill_replicate::Applied::Updated {
                        *current = Some(postcard::from_bytes(receiver.current().unwrap()).unwrap());
                    }
                    // ACK only after payload reconstruction AND application deserialization.
                    acks.extend_from_slice(
                        wire::Ack::new(decoded.receipt.attempt).unwrap().as_bytes(),
                    );
                }
                if acks.is_empty() {
                    Vec::new()
                } else {
                    vec![(usize::from(ClientChannel::MutationAcks), Bytes::from(acks))]
                }
            }
        }
    }
    fn value(&mut self) -> Dataset {
        match self {
            Self::Native(app) => app
                .world_mut()
                .query::<&Dataset>()
                .single(app.world())
                .unwrap()
                .clone(),
            Self::Rill { current, .. } => current.as_ref().unwrap().clone(),
        }
    }
}

#[derive(Clone, Default)]
struct Frame {
    incoming: Vec<Message>,
    outgoing: Vec<Message>,
    required: Option<u64>,
}
struct Trace {
    prefix: Vec<Vec<Message>>,
    frames: Vec<Frame>,
}
struct Held {
    due: usize,
    peer: usize,
    message: Message,
    target: u64,
}
fn maximum(slot: &mut Option<u64>, target: u64) {
    *slot = Some(slot.map_or(target, |old| old.max(target)));
}
fn predict(native: bool, peer: usize, messages: &[Message]) -> Vec<Message> {
    let mut acks = Vec::new();
    for (channel, body) in messages {
        if native {
            if *channel == usize::from(ServerChannel::Mutations) {
                let (_, rest) = postcard::take_from_bytes::<u8>(body).unwrap();
                acks.extend_from_slice(rest.get(..2).unwrap());
            }
        } else {
            let decoded = wire::decode(session(peer), body, byte_codecs::LIMIT * 4).unwrap();
            acks.extend_from_slice(wire::Ack::new(decoded.receipt.attempt).unwrap().as_bytes());
        }
    }
    if acks.is_empty() {
        Vec::new()
    } else {
        vec![(usize::from(ClientChannel::MutationAcks), Bytes::from(acks))]
    }
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(
        args.len(),
        8,
        "native|rill strategy codec peers case size sync|loss ack-delay"
    );
    let native = match args[0].as_str() {
        "native" => true,
        "rill" => false,
        _ => panic!("engine"),
    };
    if native {
        assert_eq!((&args[1][..], &args[2][..]), ("full", "full"));
    }
    let peers: usize = args[3].parse().unwrap();
    let size: usize = args[5].parse().unwrap();
    let loss = match args[6].as_str() {
        "sync" => false,
        "loss" => true,
        _ => panic!("trace"),
    };
    let delay: usize = args[7].parse().unwrap();
    assert!(peers > 0 && peers <= 20000 && size <= 65536 && delay <= 64);
    let ticks = 33 + delay + 8;
    let mut expected = Dataset {
        revision: 0,
        bytes: byte_codecs::initial(size),
    };
    let mut models = vec![expected.clone()];
    let mut seed = 4815162342;
    let setup = Instant::now();
    let mut prefix_out = Vec::new();
    let mut traces: Vec<_> = (0..peers)
        .map(|_| Trace {
            prefix: vec![Vec::new(); 4],
            frames: vec![Frame::default(); ticks],
        })
        .collect();
    let mut engine = if native {
        let mut prototype = native_client();
        for _ in 0..4 {
            prototype.update();
            prefix_out.push(native_output(&mut prototype));
        }
        drop(prototype);
        let mut app = native_app(true);
        app.world_mut()
            .resource_mut::<NextState<ServerState>>()
            .set(ServerState::Running);
        let entities: Vec<_> = (0..peers)
            .map(|_| {
                app.world_mut()
                    .spawn(ConnectedClient { max_size: 1200 })
                    .id()
            })
            .collect();
        let index: HashMap<_, _> = entities.iter().enumerate().map(|(i, e)| (*e, i)).collect();
        for (tick, messages) in prefix_out.iter().enumerate() {
            for entity in &entities {
                let mut input = app.world_mut().resource_mut::<ServerMessages>();
                for (channel, body) in messages {
                    input.insert_received(*entity, *channel, body.clone());
                }
            }
            app.update();
            for (entity, channel, body) in app
                .world_mut()
                .resource_mut::<ServerMessages>()
                .drain_sent()
            {
                assert_ne!(channel, usize::from(ServerChannel::Mutations));
                traces[index[&entity]].prefix[tick].push((channel, body));
            }
        }
        assert!(
            entities
                .iter()
                .all(|e| app.world().get::<AuthorizedClient>(*e).is_some())
        );
        let dataset = app.world_mut().spawn((Replicated, expected.clone())).id();
        Engine::Native {
            app: Box::new(app),
            entities,
            index,
            dataset,
        }
    } else {
        let config = Config {
            history: HISTORY,
            peers,
            attempts_per_peer: 64,
            ..Config::default()
        };
        let mut sender = Sender::new(
            serialize(&expected),
            Adapter::new(&args[2]),
            config,
            strategy(&args[1]),
        )
        .unwrap();
        for peer in 0..peers {
            sender.add_peer(session(peer)).unwrap();
        }
        Engine::Rill {
            sender,
            peers,
            inbox: Vec::new(),
            initial: true,
        }
    };
    let setup_ns = setup.elapsed().as_nanos();
    let setup_server_bytes: usize = traces
        .iter()
        .flat_map(|p| &p.prefix)
        .flatten()
        .map(|m| m.1.len())
        .sum();
    let setup_client_bytes: usize = prefix_out
        .iter()
        .flatten()
        .map(|m| m.1.len())
        .sum::<usize>()
        * peers;
    let mut mutation_ns = 0;
    let mut server_ns = 0;
    let mut ingest_ns = 0;
    let mut trace_ns = 0;
    let mut oracle_ns = 0;
    let mut wire_bytes = 0usize;
    let mut ack_bytes = 0usize;
    let mut dropped = 0;
    let mut delayed = 0;
    let mut duplicated = 0;
    let mut held_data = VecDeque::<Held>::new();
    let mut held_acks = VecDeque::<Held>::new();
    let mut targets = vec![None; peers];
    let generation = Instant::now();
    for tick in 0..ticks {
        if tick > 0 && tick <= 32 {
            expected.revision = tick as u64;
            byte_codecs::mutate(&mut expected.bytes, &args[4], tick, &mut seed);
            models.push(expected.clone());
            let start = Instant::now();
            engine.publish(&expected);
            mutation_ns += start.elapsed().as_nanos();
        }
        let start = Instant::now();
        let packets = engine.step();
        server_ns += start.elapsed().as_nanos();
        let start = Instant::now();
        targets.fill(None);
        for (peer, (channel, body)) in packets {
            wire_bytes += body.len();
            if loss && tick <= 32 && channel == usize::from(ServerChannel::Mutations) {
                if (tick + peer) % 11 == 0 {
                    dropped += 1;
                    continue;
                }
                if (tick + peer) % 13 == 0 {
                    delayed += 1;
                    held_data.push_back(Held {
                        due: tick + 3,
                        peer,
                        message: (channel, body),
                        target: expected.revision,
                    });
                    continue;
                }
                if (tick + peer) % 17 == 0 {
                    duplicated += 1;
                    wire_bytes += body.len();
                    traces[peer].frames[tick]
                        .incoming
                        .push((channel, body.clone()));
                }
            }
            if !native || channel == usize::from(ServerChannel::Mutations) {
                maximum(&mut targets[peer], expected.revision);
            }
            traces[peer].frames[tick].incoming.push((channel, body));
        }
        while held_data.front().is_some_and(|p| p.due <= tick) {
            let p = held_data.pop_front().unwrap();
            maximum(&mut targets[p.peer], p.target);
            traces[p.peer].frames[tick].incoming.push(p.message);
        }
        trace_ns += start.elapsed().as_nanos();
        let start = Instant::now();
        for (peer, trace) in traces.iter_mut().enumerate() {
            let replies = predict(native, peer, &trace.frames[tick].incoming);
            for message in replies {
                ack_bytes += message.1.len();
                trace.frames[tick].outgoing.push(message.clone());
                held_acks.push_back(Held {
                    due: tick + delay,
                    peer,
                    message,
                    target: targets[peer].unwrap(),
                });
            }
        }
        let mut ready = Vec::new();
        while held_acks.front().is_some_and(|p| p.due <= tick) {
            let p = held_acks.pop_front().unwrap();
            maximum(&mut traces[p.peer].frames[tick].required, p.target);
            ready.push(p);
        }
        oracle_ns += start.elapsed().as_nanos();
        let start = Instant::now();
        for p in ready {
            engine.ingest(p.peer, p.message);
        }
        ingest_ns += start.elapsed().as_nanos();
    }
    assert!(held_data.is_empty());
    let generation_ns = generation.elapsed().as_nanos();
    let messages: usize = traces
        .iter()
        .map(|p| p.frames.iter().map(|f| f.incoming.len()).sum::<usize>())
        .sum();
    let model_digest = models.iter().fold(0xcbf29ce484222325u64, |hash, model| {
        model
            .revision
            .to_le_bytes()
            .iter()
            .chain((model.bytes.len() as u64).to_le_bytes().iter())
            .chain(model.bytes.iter())
            .fold(hash, |h, byte| {
                (h ^ *byte as u64).wrapping_mul(0x100000001b3)
            })
    });
    let replay = Instant::now();
    let mut client_ns = 0;
    let mut client_ingest_ns = 0;
    let mut client_setup_ns = 0;
    let mut full_checks = 0;
    for (peer, trace) in traces.iter().enumerate() {
        let start = Instant::now();
        let mut client = Client::new(native, &args[2], peer);
        for (tick, expected) in prefix_out.iter().enumerate() {
            assert_eq!(client.step(), *expected);
            client.ingest(&trace.prefix[tick]);
        }
        client_setup_ns += start.elapsed().as_nanos();
        for (tick, frame) in trace.frames.iter().enumerate() {
            let start = Instant::now();
            client.ingest(&frame.incoming);
            client_ingest_ns += start.elapsed().as_nanos();
            let start = Instant::now();
            let output = client.step();
            client_ns += start.elapsed().as_nanos();
            assert_eq!(output, frame.outgoing, "ACK peer={peer} tick={tick}");
            let value = client.value();
            assert!(value.revision <= 32);
            assert_eq!(
                value, models[value.revision as usize],
                "dataset peer={peer} tick={tick}"
            );
            full_checks += 1;
            if let Some(target) = frame.required {
                assert!(value.revision >= target, "ACK before application");
            }
            if tick == ticks - 1 {
                assert_eq!(value, expected);
            }
        }
    }
    if loss && peers >= 8 {
        assert!(dropped > 0 && delayed > 0 && duplicated > 0);
    }
    println!(
        "{{\"engine\":\"{}\",\"strategy\":\"{}\",\"codec\":\"{}\",\"peers\":{},\"case\":\"{}\",\"size\":{},\"trace\":\"{}\",\"ack_delay\":{},\"frames\":32,\"fixed_clock_ns\":{},\"setup_ns\":{},\"setup_server_wire_bytes\":{},\"setup_client_wire_bytes\":{},\"model_digest\":{},\"publication_ns\":{},\"server_step_ns\":{},\"ack_ingest_ns\":{},\"trace_ns\":{},\"ack_oracle_ns\":{},\"generation_ns\":{},\"receiver_setup_ns\":{},\"receiver_ingest_ns\":{},\"receiver_step_ns\":{},\"replay_verification_ns\":{},\"server_wire_bytes\":{},\"ack_wire_bytes\":{},\"trace_messages\":{},\"dropped\":{},\"delayed\":{},\"duplicated\":{},\"decoder_replays\":{},\"full_value_checks\":{},\"exact_ack_frames\":{},\"verified\":true,\"timing_acceptance\":false}}",
        args[0],
        args[1],
        args[2],
        peers,
        args[4],
        size,
        args[6],
        delay,
        CLOCK,
        setup_ns,
        setup_server_bytes,
        setup_client_bytes,
        model_digest,
        mutation_ns,
        server_ns,
        ingest_ns,
        trace_ns,
        oracle_ns,
        generation_ns,
        client_setup_ns,
        client_ingest_ns,
        client_ns,
        replay.elapsed().as_nanos(),
        wire_bytes,
        ack_bytes,
        messages,
        dropped,
        delayed,
        duplicated,
        peers,
        full_checks,
        full_checks
    );
}
