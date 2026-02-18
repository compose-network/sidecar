//! Sidecar binary entrypoint.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use compose_config::SidecarArgs;
use compose_coordinator::builder::CoordinatorBuilder;
use compose_coordinator::coordinator::DefaultCoordinator;
use compose_mailbox::put_inbox::PutInboxTxBuilder;
use compose_mailbox::queue::InMemoryQueue;
use compose_metrics::SidecarMetrics;
use compose_peer::coordinator::{HttpPeerCoordinator, PeerEntry};
use compose_peer::sender::PeerMailboxSender;
use compose_publisher::PublisherConnection;
use compose_server::handlers::publisher::handle_publisher_message;
use compose_server::router::build_router;
use compose_server::state::AppState;
use compose_simulation::rpc::RpcSimulator;
use compose_simulation::types::ChainRpcConfig;
use compose_transport::client::QuicClient;
use compose_transport::config::ClientConfig;
use compose_transport::traits::Transport;
use prometheus_client::registry::Registry;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let args = SidecarArgs::parse();

    compose_tracing::init(&args.log.level, &args.log.format);

    info!("Starting sidecar");

    let mut registry = Registry::default();
    let metrics = Arc::new(SidecarMetrics::new(&mut registry));

    let (coordinator, quic_client) = build_coordinator(&args, metrics);

    coordinator.start().await?;

    let coordinator_arc = Arc::new(coordinator);

    if let Some(client) = quic_client {
        spawn_publisher_connection(coordinator_arc.clone(), client);
    }

    let state = AppState::from_arc(coordinator_arc).with_registry(registry);
    let router = build_router(state);

    let listener = TcpListener::bind(&args.server.listen_addr).await?;
    info!(addr = %args.server.listen_addr, "HTTP server listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Shutting down");
    Ok(())
}

fn build_coordinator(
    args: &SidecarArgs,
    metrics: Arc<SidecarMetrics>,
) -> (DefaultCoordinator, Option<Arc<QuicClient>>) {
    let chain_id = args.chain.chain_id();

    let mut builder = CoordinatorBuilder::new(chain_id).metrics(metrics);

    if !args.chain.rpc.is_empty() {
        let rpc_chains = vec![ChainRpcConfig {
            chain_id,
            rpc_url: args.chain.rpc.clone(),
        }];
        let mut sim = RpcSimulator::new(rpc_chains);
        if !args.chain.mailbox_address.is_empty() {
            if let Ok(addr) = args.chain.mailbox_address.parse() {
                sim = sim.with_mailbox_address(addr);
            }
        }
        builder = builder.simulator(Arc::new(sim));
    }

    let has_rpc = !args.chain.rpc.is_empty();
    let has_mailbox = !args.chain.mailbox_address.is_empty();
    let has_key = !args.chain.coordinator_key.is_empty();
    if has_rpc && has_mailbox && has_key {
        match PutInboxTxBuilder::new(
            chain_id,
            args.chain.rpc.clone(),
            args.chain.mailbox_address.clone(),
            args.chain.coordinator_key.clone(),
        ) {
            Ok(put_inbox) => {
                builder = builder.put_inbox_builder(Arc::new(put_inbox));
            }
            Err(e) => {
                warn!(error = %e, "Failed to configure putInbox builder");
            }
        }
    } else if has_mailbox || has_key {
        warn!(
            has_rpc,
            has_mailbox,
            has_coordinator_key = has_key,
            "putInbox builder disabled due to incomplete chain config"
        );
    }

    builder = builder.mailbox_queue(Arc::new(InMemoryQueue::new()));

    let peer_entries = args.peers.to_entries();
    if !peer_entries.is_empty() {
        let peers: Vec<PeerEntry> = peer_entries
            .iter()
            .map(|p| PeerEntry {
                chain_id: p.chain_id,
                addr: p.addr.clone(),
            })
            .collect();
        let pc = Arc::new(HttpPeerCoordinator::new(peers));
        builder = builder.peer_coordinator(pc);

        let mailbox_peers: Vec<PeerEntry> = peer_entries
            .iter()
            .map(|p| PeerEntry {
                chain_id: p.chain_id,
                addr: p.addr.clone(),
            })
            .collect();
        builder = builder.mailbox_sender(Arc::new(PeerMailboxSender::with_peer_entries(
            &mailbox_peers,
        )));
    }

    let quic_client = if args.publisher.enabled && !args.publisher.addr.is_empty() {
        let client_config = ClientConfig {
            addr: args.publisher.addr.clone(),
            client_id: chain_id.0.to_string(),
            reconnect_delay: Duration::from_secs(args.publisher.reconnect_delay_secs),
            max_retries: args.publisher.max_retries,
            ..Default::default()
        };
        match QuicClient::new(client_config) {
            Ok(client) => {
                let conn = PublisherConnection::new(client.clone(), chain_id);
                builder = builder.publisher(Arc::new(conn));
                Some(client)
            }
            Err(e) => {
                warn!(error = %e, "Failed to create QUIC client, running without publisher");
                None
            }
        }
    } else {
        None
    };

    (builder.build(), quic_client)
}

fn spawn_publisher_connection(coordinator: Arc<DefaultCoordinator>, client: Arc<QuicClient>) {
    tokio::spawn(async move {
        info!("Connecting to publisher");
        if let Err(e) = client.connect_with_retry().await {
            error!(error = %e, "Failed to connect to publisher after retries");
            return;
        }
        info!("Connected to publisher, starting receive loop");

        loop {
            match client.recv().await {
                Ok(data) => {
                    let coord = coordinator.clone();
                    tokio::spawn(async move {
                        handle_publisher_message(coord, data).await;
                    });
                }
                Err(e) => {
                    warn!(error = %e, "Publisher receive error, connection may be lost");
                    break;
                }
            }
        }

        warn!("Publisher receive loop ended");
    });
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("Received shutdown signal");
}
