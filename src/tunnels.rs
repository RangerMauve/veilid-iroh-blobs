use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Ok;
use anyhow::{anyhow, Result};
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use veilid_core::{RouteId, RoutingContext, Target, VeilidUpdate};

pub struct TunnelManager {
    router: RoutingContext,
    route_id: RouteId,
    id_counter: u32,
    message_inputs: Option<Sender<TunnelMessage>>,
    tunnels: HashMap<u32, Arc<Tunnel>>,
}

pub struct Tunnel {
    id: u32,
    target: RouteId,
    remote_to_local: Sender<Bytes>,
    local_to_remote: Sender<TunnelMessage>,
    from_remote_to_local: Receiver<Bytes>,
}

impl Tunnel {
    pub async fn write(self, bytes: Bytes) -> Result<()> {
        let message = TunnelMessage {
            id: self.id,
            bytes,
            target: self.target,
        };

        self.local_to_remote.send(message).await?;

        return Ok(());
    }
}

struct TunnelMessage {
    id: u32,
    bytes: Bytes,
    target: RouteId,
}

impl TunnelMessage {
    fn to_bytes(&self) -> Bytes {
        let mut buffer = BytesMut::with_capacity(self.bytes.len() + 4);
        buffer.put_u32(self.id);
        buffer.put(self.bytes.clone());
        return buffer.freeze();
    }

    fn from_bytes(buffer: Bytes, target: RouteId) -> Self {
        let mut buffer = Bytes::from(buffer);
        let id = buffer.get_u32();
        let bytes = Bytes::copy_from_slice(buffer.chunk());

        return TunnelMessage { id, bytes, target };
    }
}

impl TunnelManager {
    pub fn new(router: RoutingContext, route_id: RouteId) -> Self {
        TunnelManager {
            router,
            route_id,
            id_counter: 0,
            message_inputs: None,
            tunnels: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let (tx, mut rx) = channel::<TunnelMessage>(8);

        self.message_inputs = Some(tx);

        while let Some(message) = rx.recv().await {
            let bytes = message.to_bytes().to_vec();
            let target = Target::PrivateRoute(message.target);
            self.router
                .app_call(target, bytes)
                .await
                .map_err(|e| anyhow!("Failed to initialize Veilid API: {}", e))?;

            // TODO: Report errors somewhere instead of breaking? Break the channel?
            match self.message_inputs {
                None => {
                    return Ok(());
                }
                Some(_) => {
                    // Keep goin!
                }
            }
        }

        self.message_inputs = None;

        Ok(())
    }

    pub async fn stop(&mut self) {
        self.message_inputs = None;
    }

    pub fn wants_message(&self, message: VeilidUpdate) -> bool {
        match &message {
            VeilidUpdate::AppCall(msg) => {
                match msg.route_id() {
                    Some(route_id) => {
                        return route_id == &self.route_id;
                    }
                    None => {
                        return false;
                    }
                };
            }
            _ => false,
        }
    }

    pub async fn handle_update(&mut self, veilid_update: VeilidUpdate) -> Result<()> {
        match self.message_inputs.clone() {
            None => {
                return Err(anyhow!(
                    "Must call TunnelManager::run() before opening tunnels"
                ));
            }
            Some(message_inputs) => match &veilid_update {
                VeilidUpdate::AppCall(appcall) => {
                    match appcall.route_id() {
                        Some(route_id) => {
                            let data = appcall.message();
                            let target = route_id.clone() as RouteId;
                            let bytes = Bytes::copy_from_slice(data);
                            let message = TunnelMessage::from_bytes(bytes, target);

                            let id = message.id;
                            let tunnel = self.get_tunnel(id);
                            match tunnel {
                                Some(tunnel) => {
                                    tunnel.remote_to_local.send(message.bytes).await?;
                                }
                                None => {
                                    // TODO: Check for ping, respond with pong
                                    let _tunnel = self.add_tunnel(target, id, message_inputs);
                                    return Ok(());
                                }
                            }

                            return Ok(());
                        }
                        None => {
                            return Err(anyhow!("Got message without route id"));
                        }
                    };
                }
                _ => {
                    return Err(anyhow!(
                        "Invalid update type passed to TunnelManager, expected AppCall"
                    ))
                }
            },
        }
    }

    pub fn get_tunnel(&mut self, id: u32) -> Option<Arc<Tunnel>> {
        return self.tunnels.get(&id).cloned();
    }

    fn add_tunnel(
        &mut self,
        target: RouteId,
        id: u32,
        message_inputs: Sender<TunnelMessage>,
    ) -> Arc<Tunnel> {
        // TODO: bigger buffer?
        let (remote_to_local, from_remote_to_local) = channel(8);
        let local_to_remote = message_inputs.clone();

        let tunnel = Arc::new(Tunnel {
            id,
            remote_to_local,
            local_to_remote,
            from_remote_to_local,
            target,
        });

        self.tunnels.insert(id, Arc::clone(&tunnel));

        return tunnel;
    }

    pub async fn open_to(&mut self, target: RouteId) -> Result<Arc<Tunnel>> {
        match self.message_inputs.clone() {
            None => {
                return Err(anyhow!(
                    "Must call TunnelManager::run() before opening tunnels"
                ));
            }
            Some(message_inputs) => {
                let id = self.next_id();

                let tunnel = self.add_tunnel(target, id, message_inputs);
                // send ping and wait for pong
                return Ok(tunnel);
            }
        }
    }

    fn next_id(&mut self) -> u32 {
        self.id_counter += 1;
        return self.id_counter;
    }
}
