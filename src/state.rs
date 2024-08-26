use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;

pub type SafeSharedState = Arc<Mutex<SharedState>>;
pub type InMemoryConsumers = Arc<Mutex<HashMap<String, Box<dyn AsyncWrite + Unpin + Send>>>>;
pub type InMemoryQueue = Arc<Mutex<VecDeque<Vec<u8>>>>;

pub struct SharedState {
    pub queue: InMemoryQueue,
    pub consumers: InMemoryConsumers,
}

impl SharedState {
    pub fn new() -> SafeSharedState {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let consumers = Arc::new(Mutex::new(HashMap::new()));

        let state = Self { queue, consumers };

        Arc::new(Mutex::new(state))
    }
}
