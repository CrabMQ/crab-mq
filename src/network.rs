use crate::message::{MessageType, MESSAGE_BUFFER_SIZE};
use crate::state::SafeSharedState;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn create_listener(addr: &str) -> Result<TcpListener, std::io::Error> {
    TcpListener::bind(addr).await
}

pub async fn handle_connection(
    state: SafeSharedState,
    (mut socket, addr): (TcpStream, std::net::SocketAddr),
) {
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
                handle_subscribe(&state, addr.to_string(), Box::new(socket)).await;
                break;
            }
        }
    }
}

pub async fn handle_publish(state: &SafeSharedState, message: &[u8]) {
    let state = state.lock().await;
    let mut queue = state.queue.lock().await;
    log::debug!("Publishing message: {:?}", std::str::from_utf8(message));
    queue.push_back(message.to_vec());
}

pub async fn handle_subscribe(
    state: &SafeSharedState,
    addr: String,
    socket: Box<dyn AsyncWrite + Unpin + Send>,
) {
    let state = state.lock().await;
    let mut consumers = state.consumers.lock().await;
    consumers.insert(addr.to_string(), socket);
}

pub async fn distribute_messages(state: SafeSharedState) {
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

#[cfg(test)]
mod tests {
    use crate::state::SharedState;

    use super::*;

    #[tokio::test]
    async fn test_handle_publish() {
        let state = SharedState::new();
        let message = b"Hello, World!";

        handle_publish(&state, message).await;

        let state = state.lock().await;
        let queue = state.queue.lock().await;

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.front().unwrap(), message);
    }

    #[tokio::test]
    async fn test_handle_subscribe() {
        let stream: Vec<u8> = Vec::new();

        let state = SharedState::new();
        let addr = "127.0.0.1:8080".to_string();

        handle_subscribe(&state, addr.clone(), Box::new(stream)).await;

        let state = state.lock().await;
        let consumers = state.consumers.lock().await;

        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers.contains_key(&addr), true);
    }
}
