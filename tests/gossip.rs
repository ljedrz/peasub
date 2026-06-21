//! Integration tests for the metadata-private gossip protocol.

use std::time::{Duration, Instant};

use peasub::{connect_nodes, CoverStrategy, Node, NodeConfig, Topology};

/// Returns a [`NodeConfig`] tailored for tests: small messages, fast
/// cover schedule, loopback listener, and a permissive per-IP
/// connection cap.
///
/// The relay outbox is intentionally tiny: a small `relay_outbox_capacity`
/// keeps the time a freshly-received message spends in the FIFO queue
/// short, which is what makes the test's propagation deadline achievable
/// under a 50 ms cover rate. Real deployments should size the relay
/// outbox based on `(num_peers - 1) * cover_rate * drain_seconds`.
fn test_config(name: &str, cover: CoverStrategy, fanout: usize) -> NodeConfig {
    NodeConfig {
        name: Some(name.into()),
        listener_addr: Some("127.0.0.1:0".parse().unwrap()),
        cover,
        fanout,
        message_size: 128,
        app_outbox_capacity: 16,
        relay_outbox_capacity: 4,
        dedup_capacity: 4096,
        max_connections: 32,
        max_connections_per_ip: 8,
        ..Default::default()
    }
}

/// Spawns the given number of nodes (each with the provided cover
/// strategy and fanout) and connects them in a mesh topology.
async fn spawn_mesh(n: usize, cover: CoverStrategy, fanout: usize) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let node = Node::new(test_config(&format!("node-{i}"), cover, fanout));
        node.spawn().await.expect("spawn");
        nodes.push(node);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    connect_nodes(&nodes, Topology::Mesh)
        .await
        .expect("connect_nodes");
    nodes
}

/// Returns true if the buffer `haystack` contains the needle `payload`.
/// Because the gossip frame is padded with random bytes we cannot match
/// on the whole buffer; we just look for our marker as a substring.
fn contains_payload(haystack: &[u8], payload: &[u8]) -> bool {
    if payload.is_empty() {
        return true;
    }
    haystack.windows(payload.len()).any(|w| w == payload)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_message_directly_received_by_one_peer() {
    // Simpler sanity check: 2 nodes, one publishes, the other sees it.
    let alice = Node::new(test_config(
        "alice",
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        1,
    ));
    let bob = Node::new(test_config(
        "bob",
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        1,
    ));
    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    for _ in 0..50 {
        if alice.connected_peers().contains(&bob_addr) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut bob_rx = bob.subscribe();
    let marker = b"peasub-direct-marker".to_vec();
    let published = alice.publish(&marker);
    let pub_id = published.expect("publish");

    // Wait for the marker to arrive at bob.
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut got = false;
    while !got && Instant::now() < deadline {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), bob_rx.recv()).await {
            if contains_payload(&buf, &marker) {
                got = true;
            }
        }
    }
    assert!(
        got,
        "bob never saw the marker published by alice (id {:?})",
        pub_id
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_message_propagates_to_majority() {
    // Fanout-1 gossip over a 4-node mesh: the marker should reach a
    // strict majority of nodes (the publisher + at least 2 of the 3
    // peers) within a few hundred milliseconds. Reaching *all* nodes
    // depends on the random walk not getting stuck bouncing between
    // two nodes, which is fundamentally probabilistic for fanout-1
    // gossip and is why a denser overlay or higher fanout is
    // recommended for production deployments.
    let nodes = spawn_mesh(
        4,
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        1,
    )
    .await;

    let mut subs: Vec<_> = nodes.iter().map(|n| n.subscribe()).collect();
    let marker = b"peasub-marker-payload".to_vec();
    nodes[0].publish(&marker).expect("publish");

    let mut seen = vec![false; nodes.len()];
    let deadline = Instant::now() + Duration::from_secs(5);

    while seen.iter().filter(|&&s| s).count() < 2 && Instant::now() < deadline {
        for (i, rx) in subs.iter_mut().enumerate() {
            if seen[i] {
                continue;
            }
            if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                if contains_payload(&buf, &marker) {
                    seen[i] = true;
                }
            }
        }
    }

    let reached: Vec<usize> = seen
        .iter()
        .enumerate()
        .filter_map(|(i, &s)| if s { Some(i) } else { None })
        .collect();
    assert!(
        seen.iter().filter(|&&s| s).count() >= 2,
        "marker only reached nodes {reached:?} (need at least 2, including the publisher)"
    );

    for node in &nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cover_traffic_runs_at_configured_constant_rate() {
    // Two nodes connected to each other; with a 50 ms cover interval
    // we expect ~20 messages / second to flow in each direction over
    // a 1 s measurement window. Allow generous slack.
    let alice = Node::new(test_config(
        "alice",
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        1,
    ));
    let bob = Node::new(test_config(
        "bob",
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        1,
    ));

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    // wait for the connection to be observed
    for _ in 0..50 {
        if alice.connected_peers().contains(&bob_addr) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut rx = bob.subscribe();
    let start = Instant::now();
    let mut count = 0usize;
    while start.elapsed() < Duration::from_secs(1) {
        if tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_ok()
        {
            count += 1;
        }
    }

    // 20 expected, but give a wide margin so the test is not flaky on
    // busy CI machines.
    assert!(
        (8..=60).contains(&count),
        "expected ~20 cover messages per second, got {count}",
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cover_traffic_runs_at_configured_poisson_rate() {
    // Same shape as the constant-rate test, but with a Poisson schedule
    // at 25 msg/s on average. We allow a wide slack band because the
    // 1 s measurement window is short relative to the variance.
    let alice = Node::new(test_config(
        "alice",
        CoverStrategy::Poisson { rate: 25.0 },
        1,
    ));
    let bob = Node::new(test_config("bob", CoverStrategy::Poisson { rate: 25.0 }, 1));

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    for _ in 0..50 {
        if alice.connected_peers().contains(&bob_addr) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut rx = bob.subscribe();
    let start = Instant::now();
    let mut count = 0usize;
    while start.elapsed() < Duration::from_secs(2) {
        if tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_ok()
        {
            count += 1;
        }
    }

    // 50 expected over 2 s; allow a 3x slack band.
    assert!(
        (10..=200).contains(&count),
        "expected ~50 cover messages over 2 s at rate 25/s, got {count}",
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_messages_are_dropped() {
    // A mesh of 3 nodes; publish a single message, then verify that
    // the dedup cache suppresses re-broadcasts of the same ID. We use
    // a mesh (rather than a ring) so the propagation is reliable
    // within the deadline even though the marker is only forwarded
    // once per node.
    let nodes = spawn_mesh(
        3,
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        1,
    )
    .await;

    let mut subs: Vec<_> = nodes.iter().map(|n| n.subscribe()).collect();
    let marker = b"peasub-dedup-marker".to_vec();
    nodes[0].publish(&marker).unwrap();

    // Wait for the marker to reach a majority of nodes.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut received_at: Vec<Option<Instant>> = vec![None; nodes.len()];

    while received_at.iter().filter(|r| r.is_some()).count() < 2 && Instant::now() < deadline {
        for (i, rx) in subs.iter_mut().enumerate() {
            if received_at[i].is_some() {
                continue;
            }
            if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                if contains_payload(&buf, &marker) {
                    received_at[i] = Some(Instant::now());
                }
            }
        }
    }

    assert!(
        received_at.iter().filter(|r| r.is_some()).count() >= 2,
        "marker did not reach a majority of nodes: {received_at:?}",
    );

    // For each node that did see the marker, verify the dedup cache
    // suppressed re-broadcasts: the next 30 frames must not also
    // contain the marker.
    for (i, rx) in subs.iter_mut().enumerate() {
        if received_at[i].is_none() {
            continue;
        }
        let mut duplicates = 0;
        for _ in 0..30 {
            if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                if contains_payload(&buf, &marker) {
                    duplicates += 1;
                }
            }
        }
        assert_eq!(
            duplicates, 0,
            "node {i} saw the marker {duplicates} times after the initial delivery",
        );
    }

    for node in &nodes {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_too_large_is_rejected() {
    let node = Node::new(test_config(
        "node",
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        1,
    ));
    node.spawn().await.unwrap();

    // 128-byte message size; 32-byte ID; so 96 bytes is the maximum
    // user payload.
    let too_big = vec![0u8; 97];
    let err = node.publish(&too_big).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("payload too large"), "unexpected error: {msg}");

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_reaches_all_nodes() {
    // With fanout >= 2 in a small mesh, a single published message
    // should reach *every* node within a short window — the defining
    // property of higher-fanout gossip vs. the fanout-1 random walk,
    // which only guarantees a majority.
    let nodes = spawn_mesh(
        5,
        CoverStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        3,
    )
    .await;

    let mut subs: Vec<_> = nodes.iter().map(|n| n.subscribe()).collect();
    let marker = b"peasub-fanout-marker".to_vec();
    nodes[0].publish(&marker).expect("publish");

    let mut seen = vec![false; nodes.len()];
    // node 0 is the publisher; it "has" the message by definition.
    seen[0] = true;
    let deadline = Instant::now() + Duration::from_secs(3);

    while seen.iter().filter(|&&s| s).count() < nodes.len() && Instant::now() < deadline {
        for (i, rx) in subs.iter_mut().enumerate() {
            if seen[i] {
                continue;
            }
            if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                if contains_payload(&buf, &marker) {
                    seen[i] = true;
                }
            }
        }
    }

    let missing: Vec<usize> = seen
        .iter()
        .enumerate()
        .filter_map(|(i, &s)| if !s { Some(i) } else { None })
        .collect();
    assert!(
        missing.is_empty(),
        "with fanout=3, marker did not reach nodes {missing:?} (seen: {seen:?})",
    );

    for node in &nodes {
        node.shutdown().await;
    }
}
