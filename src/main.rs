pub mod message;
pub mod network;
pub mod state;

use crate::network::{create_listener, distribute_messages, handle_connection};
use crate::state::SharedState;
use log;
use std::error::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    log::info!("CrabMQ Starting...");

    let addr = std::env::args().nth(1).unwrap_or_else(|| {
        log::error!("Usage: queue <addr>");
        std::process::exit(1);
    });

    let listener = create_listener(&addr).await?;

    log::info!("Queue listening on: {}", addr);

    let state = SharedState::new();

    let state_for_distribution = Arc::clone(&state);

    tokio::spawn(async move {
        loop {
            let state = Arc::clone(&state_for_distribution);

            log::debug!("Distributing messages");

            distribute_messages(state).await;

            // todo: make this configurable
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    });

    loop {
        let (socket, addr) = listener.accept().await?;

        log::debug!("Accepted connection from: {}", addr);

        let state = Arc::clone(&state);

        tokio::spawn(async move {
            handle_connection(state, (socket, addr)).await;
        });
    }
}
