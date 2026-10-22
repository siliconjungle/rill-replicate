//! Staged native capability/memory fixture, not a cross-engine speed verdict.
#[path = "../common.rs"]
mod common;
use bevy::{prelude::*, state::app::StatesPlugin};
use bevy_replicon::{bytes::Bytes, prelude::*, shared::backend::channels::ServerChannel};
use common::{Change, Dataset};
use std::{
    collections::{HashMap, VecDeque},
    time::Instant,
};

fn role_app(diff: bool, server: bool) -> App {
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
    app.finish();
    app
}

struct Held {
    due: usize,
    peer: usize,
    channel: usize,
    body: Bytes,
}
#[derive(Default)]
struct Measures {
    server_ns: u128,
    outbound_ns: u128,
    client_ns: u128,
    acks_ns: u128,
    mutation_ns: u128,
    server_bytes: usize,
    ack_bytes: usize,
    dropped: usize,
    duplicated: usize,
    delayed: usize,
    checks: usize,
}
struct Network {
    entities: Vec<Entity>,
    index: HashMap<Entity, usize>,
    acks: VecDeque<Held>,
    mutations: VecDeque<Held>,
}
impl Network {
    fn server_to_clients(
        &mut self,
        server: &mut App,
        clients: &mut [App],
        tick: usize,
        loss: bool,
        m: &mut Measures,
    ) {
        let t = Instant::now();
        for (entity, channel, body) in server
            .world_mut()
            .resource_mut::<ServerMessages>()
            .drain_sent()
        {
            let peer = *self.index.get(&entity).expect("known native peer");
            m.server_bytes += body.len();
            if loss && channel == usize::from(ServerChannel::Mutations) {
                if (tick + peer) % 11 == 0 {
                    m.dropped += 1;
                    continue;
                }
                if (tick + peer) % 13 == 0 {
                    self.mutations.push_back(Held {
                        due: tick + 3,
                        peer,
                        channel,
                        body,
                    });
                    m.delayed += 1;
                    continue;
                }
                if (tick + peer) % 17 == 0 {
                    clients[peer]
                        .world_mut()
                        .resource_mut::<ClientMessages>()
                        .insert_received(channel, body.clone());
                    m.server_bytes += body.len();
                    m.duplicated += 1;
                }
            }
            clients[peer]
                .world_mut()
                .resource_mut::<ClientMessages>()
                .insert_received(channel, body);
        }
        while self.mutations.front().is_some_and(|p| p.due <= tick) {
            let packet = self.mutations.pop_front().unwrap();
            clients[packet.peer]
                .world_mut()
                .resource_mut::<ClientMessages>()
                .insert_received(packet.channel, packet.body);
        }
        m.outbound_ns += t.elapsed().as_nanos();
    }
    fn clients_to_server(
        &mut self,
        server: &mut App,
        clients: &mut [App],
        tick: usize,
        delay: usize,
        m: &mut Measures,
    ) {
        let t = Instant::now();
        for (peer, client) in clients.iter_mut().enumerate() {
            for (channel, body) in client
                .world_mut()
                .resource_mut::<ClientMessages>()
                .drain_sent()
            {
                m.ack_bytes += body.len();
                self.acks.push_back(Held {
                    due: tick + delay,
                    peer,
                    channel,
                    body,
                });
            }
        }
        while self.acks.front().is_some_and(|p| p.due <= tick) {
            let packet = self.acks.pop_front().unwrap();
            server
                .world_mut()
                .resource_mut::<ServerMessages>()
                .insert_received(self.entities[packet.peer], packet.channel, packet.body);
        }
        m.acks_ns += t.elapsed().as_nanos();
    }
}
fn update_clients(clients: &mut [App], m: &mut Measures) {
    let t = Instant::now();
    for client in clients {
        client.update();
    }
    m.client_ns += t.elapsed().as_nanos();
}
fn update_server(server: &mut App, m: &mut Measures) {
    let t = Instant::now();
    server.update();
    m.server_ns += t.elapsed().as_nanos();
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 4, "full|diff clients sync|loss ack-delay");
    let diff = match args[0].as_str() {
        "full" => false,
        "diff" => true,
        _ => panic!("mode"),
    };
    let count: usize = args[1].parse().unwrap();
    assert!(count > 0);
    let loss = match args[2].as_str() {
        "sync" => false,
        "loss" => true,
        _ => panic!("trace"),
    };
    let ack_delay: usize = args[3].parse().unwrap();
    assert!(ack_delay <= 64);
    let setup = Instant::now();
    let mut server = role_app(diff, true);
    let mut clients: Vec<_> = (0..count).map(|_| role_app(diff, false)).collect();
    server
        .world_mut()
        .resource_mut::<NextState<ServerState>>()
        .set(ServerState::Running);
    let entities: Vec<_> = clients
        .iter_mut()
        .map(|client| {
            client
                .world_mut()
                .resource_mut::<NextState<ClientState>>()
                .set(ClientState::Connected);
            server
                .world_mut()
                .spawn(ConnectedClient { max_size: 1200 })
                .id()
        })
        .collect();
    let index = entities.iter().enumerate().map(|(i, e)| (*e, i)).collect();
    let mut net = Network {
        entities,
        index,
        acks: VecDeque::new(),
        mutations: VecDeque::new(),
    };
    let mut ignored = Measures::default();
    // All peers handshake in batches. No server tick or retain-sent scan per peer.
    for tick in 0..4 {
        update_clients(&mut clients, &mut ignored);
        net.clients_to_server(&mut server, &mut clients, tick, 0, &mut ignored);
        update_server(&mut server, &mut ignored);
        net.server_to_clients(&mut server, &mut clients, tick, false, &mut ignored);
    }
    assert!(
        net.entities
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
    let mut m = Measures::default();
    let start = Instant::now();
    // Keep ACK FIFO while delaying it; only native unreliable mutations are lost.
    for tick in 0..(33 + ack_delay + 8) {
        if tick > 0 && tick <= 32 {
            let index = tick * 13 % expected.bytes.len();
            let change = Change {
                revision: tick as u64,
                edits: vec![(index, tick as u8)],
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
            m.mutation_ns += t.elapsed().as_nanos();
        }
        update_server(&mut server, &mut m);
        net.server_to_clients(&mut server, &mut clients, tick, loss && tick <= 32, &mut m);
        update_clients(&mut clients, &mut m);
        // Independent full-value oracle, outside server and client update timings.
        for client in &mut clients {
            let mut query = client.world_mut().query::<&Dataset>();
            let state = query.single(client.world()).unwrap();
            assert!(state.revision <= 32);
            assert_eq!(*state, models[state.revision as usize]);
            m.checks += 1;
            if tick == 0 {
                assert_eq!(state.revision, 0);
            }
            if !loss && ack_delay == 0 && tick <= 32 {
                assert_eq!(*state, expected);
            }
            if tick == 32 + ack_delay + 8 {
                assert_eq!(*state, expected);
            }
        }
        net.clients_to_server(&mut server, &mut clients, tick, ack_delay, &mut m);
    }
    assert!(net.mutations.is_empty());
    if loss && count >= 8 {
        assert!(m.dropped > 0 && m.delayed > 0 && m.duplicated > 0);
    }
    println!(
        "{{\"mode\":\"{}\",\"clients\":{},\"trace\":\"{}\",\"ack_delay\":{},\"frames\":32,\"setup_ns\":{},\"data_wall_ns\":{},\"mutation_ns\":{},\"server_update_ns\":{},\"outbound_route_ns\":{},\"client_update_ns\":{},\"ack_route_ns\":{},\"server_wire_bytes\":{},\"ack_wire_bytes\":{},\"dropped\":{},\"duplicated\":{},\"delayed\":{},\"full_value_checks\":{},\"verified\":true,\"timing_acceptance\":false}}",
        args[0],
        count,
        args[2],
        ack_delay,
        setup_ns,
        start.elapsed().as_nanos(),
        m.mutation_ns,
        m.server_ns,
        m.outbound_ns,
        m.client_ns,
        m.acks_ns,
        m.server_bytes,
        m.ack_bytes,
        m.dropped,
        m.duplicated,
        m.delayed,
        m.checks
    );
}
