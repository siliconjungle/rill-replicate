//! Actual upstream server/client serialization and decoding; capability proof only.
mod common;
use bevy::{prelude::*, state::app::StatesPlugin};
use bevy_replicon::{
    prelude::*, shared::backend::channels::ServerChannel, test_app::ServerTestAppExt,
};
use common::{Change, Dataset};

fn app(diff: bool) -> App {
    let mut app = App::new();
    app.add_plugins((
        MinimalPlugins,
        StatesPlugin,
        RepliconPlugins.set(ServerPlugin::new(PostUpdate)),
    ));
    if diff {
        app.replicate_diff::<Dataset>();
    } else {
        app.replicate::<Dataset>();
    }
    app.finish();
    app
}

fn send(server: &mut App, clients: &mut [App]) {
    // Upstream test helper is fine for small proofs. Its retain-sent scan is not
    // suitable for an eventual 20k performance harness; use bulk routing there.
    for client in clients {
        server.exchange_with_client(client);
        client.update();
        server.exchange_with_client(client);
    }
}
fn values(client: &mut App) -> Vec<Dataset> {
    client
        .world_mut()
        .query::<&Dataset>()
        .iter(client.world())
        .cloned()
        .collect()
}

fn proof(diff: bool, loss: bool) {
    let mut server = app(diff);
    let mut clients: Vec<_> = (0..4).map(|_| app(diff)).collect();
    for client in &mut clients {
        server.connect_client(client);
    }
    let mut expected = Dataset {
        revision: 0,
        bytes: (0..2048).map(|i| (i / 8) as u8).collect(),
    };
    let mut models = vec![expected.clone()];
    let entity = server
        .world_mut()
        .spawn((Replicated, expected.clone()))
        .id();
    server.update();
    send(&mut server, &mut clients);
    for client in &mut clients {
        assert_eq!(values(client), [expected.clone()]);
    }
    let mut dropped = 0;
    for tick in 1..=32 {
        let index = tick as usize * 13 % expected.bytes.len();
        let change = Change {
            revision: tick,
            edits: vec![(index, tick as u8)],
        };
        expected.apply_diff(&change).unwrap();
        models.push(expected.clone());
        if diff {
            server
                .world_mut()
                .entity_mut(entity)
                .apply_diff::<Dataset>(change)
                .unwrap();
        } else {
            *server.world_mut().get_mut::<Dataset>(entity).unwrap() = expected.clone();
        }
        server.update();
        if loss && tick % 5 == 0 {
            server
                .world_mut()
                .resource_mut::<ServerMessages>()
                .retain_sent(|(_, channel, _)| {
                    if *channel == usize::from(ServerChannel::Mutations) {
                        dropped += 1;
                        false
                    } else {
                        true
                    }
                });
        }
        send(&mut server, &mut clients);
        for client in &mut clients {
            let current = values(client);
            assert_eq!(current.len(), 1);
            let state = &current[0];
            assert!(state.revision <= tick);
            assert_eq!(*state, models[state.revision as usize]);
            if !loss || tick % 5 != 0 {
                assert_eq!(*state, expected);
            }
        }
    }
    assert!(!loss || dropped > 0);
    server.world_mut().despawn(entity);
    server.update();
    send(&mut server, &mut clients);
    for client in &mut clients {
        assert!(values(client).is_empty());
    }
    server.world_mut().spawn((Replicated, expected.clone()));
    server.update();
    send(&mut server, &mut clients);
    for client in &mut clients {
        assert_eq!(values(client), [expected.clone()]);
    }
    server.disconnect_client(&mut clients[0]);
    // Fresh receiver incarnation, as in Rill's Session API. Replicon preserves
    // prior remote entities on disconnect; reusing an App needs app cleanup.
    clients[0] = app(diff);
    server.connect_client(&mut clients[0]);
    server.update();
    send(&mut server, &mut clients);
    for client in &mut clients {
        assert_eq!(values(client), [expected.clone()]);
    }
    println!(
        "{{\"engine\":\"bevy_replicon-0.45.2\",\"native_diff\":{diff},\"loss\":{loss},\"clients\":4,\"revisions\":32,\"dropped_mutations\":{dropped},\"complete_values\":true,\"despawn_reinsert\":true,\"reconnect\":true,\"timing_eligible\":false}}"
    );
}
fn main() {
    for diff in [false, true] {
        for loss in [false, true] {
            proof(diff, loss);
        }
    }
}
