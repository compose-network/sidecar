//! Sidecar binary entrypoint and runtime wiring.

mod adapters;
mod handlers;

use std::sync::Arc;

use anyhow::Result;
use compose_config::SidecarConfig;
use compose_coordinator::builder::CoordinatorBuilder;
use compose_coordinator::coordinator::DefaultCoordinator;
use compose_mailbox::queue::InMemoryQueue;
use compose_peer::coordinator::{HttpPeerCoordinator, PeerEntry};
use compose_server::router::build_router;
use compose_server::state::AppState;
use compose_simulation::rpc::RpcSimulator;
use compose_simulation::types::ChainRpcConfig;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/config.yaml".to_string());

    let config = compose_config::load(Some(&config_path))?;

    compose_tracing::init(&config.log.level, &config.log.format);

    info!("Starting sidecar");

    let coordinator = build_coordinator(&config);

    coordinator.start().await?;

    let state = AppState::new(coordinator);
    let router = build_router(state);

    let listener = TcpListener::bind(&config.server.listen_addr).await?;
    info!(addr = %config.server.listen_addr, "HTTP server listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Shutting down");
    Ok(())
}

fn build_coordinator(config: &SidecarConfig) -> DefaultCoordinator {
    let chain_id = config
        .chains
        .list
        .first()
        .map(|c| c.chain_id())
        .unwrap_or(compose_primitives::ChainId(0));

    let mut builder = CoordinatorBuilder::new(chain_id);

    // Set up simulator from chain RPC configs.
    let rpc_chains: Vec<ChainRpcConfig> = config
        .chains
        .list
        .iter()
        .map(|c| ChainRpcConfig {
            chain_id: c.chain_id(),
            rpc_url: c.rpc.clone(),
        })
        .collect();
    if !rpc_chains.is_empty() {
        builder = builder.simulator(Arc::new(RpcSimulator::new(rpc_chains)));
    }

    // Set up mailbox queue.
    builder = builder.mailbox_queue(Arc::new(InMemoryQueue::new()));

    // Set up peer coordinator.
    if !config.peers.sidecars.is_empty() {
        let peers: Vec<PeerEntry> = config
            .peers
            .sidecars
            .iter()
            .map(|p| PeerEntry {
                chain_id: compose_primitives::ChainId(p.chain_id),
                addr: p.addr.clone(),
            })
            .collect();
        builder = builder.peer_coordinator(Arc::new(HttpPeerCoordinator::new(peers)));
    }

    builder.build()
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C handler");
    info!("Received shutdown signal");
}
