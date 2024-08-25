use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug)]
enum MessageType {
    Publish,
    Subscribe,
}

const MESSAGE_BUFFER_SIZE: usize = 1024;

const PUBLISH_BYTE: u8 = 0;
const SUBSCRIBE_BYTE: u8 = 1;

impl MessageType {
    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            PUBLISH_BYTE => Some(Self::Publish),
            SUBSCRIBE_BYTE => Some(Self::Subscribe),
            _ => None,
        }
    }
}

type SafeSharedState = Arc<Mutex<SharedState>>;
type InMemoryConsumers = Arc<Mutex<HashMap<String, TcpStream>>>;
type InMemoryQueue = Arc<Mutex<VecDeque<Vec<u8>>>>;

#[derive(Clone)]
struct SharedState {
    queue: InMemoryQueue,
    consumers: InMemoryConsumers,
}

impl SharedState {
    pub fn new() -> SafeSharedState {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let consumers = Arc::new(Mutex::new(HashMap::new()));

        let state = Self { queue, consumers };

        Arc::new(Mutex::new(state))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    log::info!("CrabMQ Starting...");

    let addr = std::env::args().nth(1).unwrap_or_else(|| {
        log::error!("Usage: queue <addr>");
        std::process::exit(1);
    });

    let listener = TcpListener::bind(&addr).await.unwrap_or_else(|err| {
        log::error!("Failed to bind to address [{}], error: {}", addr, err);
        std::process::exit(1);
    });

    log::info!("Queue listening on: {}", addr);

    let state = SharedState::new();

    let state_for_distribution = Arc::clone(&state);

    tokio::spawn(async move {
        loop {
            let state = Arc::clone(&state_for_distribution);

            log::debug!("Distributing messages");

            distribute_messages(state).await;

            // todo: make the time between distribution configurable
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

async fn handle_connection(state: SafeSharedState, (mut socket, addr): (TcpStream, SocketAddr)) {
    loop {
        let mut buf = [0; MESSAGE_BUFFER_SIZE];

        let buf_len = socket.read(&mut buf).await.unwrap_or_else(|err| {
            log::error!("Failed to read from socket, error: {}", err);
            0
        });

        log::debug!("Received {} bytes, from {}", buf_len, addr);

        if buf_len == 0 {
            log::info!("Connection closed with: {}", addr);
            break;
        }

        let message_type = MessageType::from_byte(buf[0]);

        if message_type.is_none() {
            log::error!(
                "Invalid message type received: {:?}",
                std::str::from_utf8(&buf[1..buf_len])
            );
            continue;
        }

        log::info!("Received connection of: {:?}", message_type);

        match message_type.unwrap() {
            MessageType::Publish => handle_publish(&state, &buf[1..buf_len]).await,
            MessageType::Subscribe => {
                handle_subscribe(&state, addr, socket).await;
                break;
            }
        }
    }
}

async fn handle_publish(state: &SafeSharedState, message: &[u8]) {
    let state = state.lock().await;
    let mut queue = state.queue.lock().await;
    log::debug!("Publishing message: {:?}", std::str::from_utf8(message));
    queue.push_back(message.to_vec());
}

async fn handle_subscribe(state: &SafeSharedState, addr: SocketAddr, socket: TcpStream) {
    let state = state.lock().await;
    let mut consumers = state.consumers.lock().await;
    consumers.insert(addr.to_string(), socket);
}

async fn distribute_messages(state: SafeSharedState) {
    let state = state.lock().await;
    let mut queue = state.queue.lock().await;

    if queue.is_empty() {
        log::debug!("Queue is empty, skipping distribution");
        return;
    }

    let mut consumers = state.consumers.lock().await;

    if consumers.is_empty() {
        log::debug!("No consumers available, skipping distribution");
        return;
    }

    // we can unwrap here because we checked if the queue is empty
    let message = queue.pop_front().unwrap();

    for (addr, consumer) in consumers.iter_mut() {
        match consumer.write(&message).await {
            Ok(_) => {
                log::debug!(
                    "Message {:?} distributed successfully to: {}",
                    std::str::from_utf8(&message),
                    addr
                );
            }
            Err(err) => {
                log::error!(
                    "Failed to distribute message {:?} to: {}, error: {}",
                    std::str::from_utf8(&message),
                    addr,
                    err
                );
            }
        };
    }
}
