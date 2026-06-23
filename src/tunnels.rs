use anyhow::{anyhow, Result};
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tracing::error;
use veilid_core::OperationId;
use veilid_core::VeilidAPI;
use veilid_core::VeilidAppCall;
use veilid_core::{RouteId, RoutingContext, Target, VeilidUpdate};

pub type Tunnel = (Sender<Vec<u8>>, Receiver<Vec<u8>>);
pub type TunnelId = (RouteId, u32);
pub type OnNewTunnelCallback = Arc<dyn Fn(Tunnel) + Send + Sync>;
pub type OnRouteDisconnectedCallback = Arc<dyn Fn() + Send + Sync>;
pub type OnNewRouteCallback = Arc<dyn Fn(RouteId, Vec<u8>) + Send + Sync>;

// SAVE on a phone pad
static PING_BYTES: &[u8] = &[7, 2, 8, 3];

#[repr(u8)]
#[derive(PartialEq)]
pub enum TunnelResult {
    Success = 0,
    InvalidFormat = 1,
    Closed = 2,
}

enum TunnelError {
    InvalidFrame,
    Delivery,
}

impl TryFrom<u8> for TunnelResult {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == TunnelResult::Success as u8 => Ok(TunnelResult::Success),
            x if x == TunnelResult::InvalidFormat as u8 => Ok(TunnelResult::InvalidFormat),
            x if x == TunnelResult::Closed as u8 => Ok(TunnelResult::Closed),
            _ => Err(anyhow!("Invalid tunnel result value {v:?}")),
        }
    }
}

struct TunnelManagerInner {
    router: RoutingContext,
    veilid: VeilidAPI,
    route_id: RouteId,
    route_id_blob: Vec<u8>,
    id_counter: u32,
    senders: HashMap<TunnelId, Sender<Vec<u8>>>,
    on_route_disconnected_callback: Option<OnRouteDisconnectedCallback>,
    on_new_route_callback: Option<OnNewRouteCallback>,
}

#[derive(Clone)]
pub struct TunnelManager {
    inner: Arc<Mutex<TunnelManagerInner>>,
    veilid: VeilidAPI,
    on_new_tunnel: Option<OnNewTunnelCallback>,
}

impl TunnelManagerInner {
    async fn notify_bytes(&self, id: &TunnelId, bytes: &[u8]) -> Result<()> {
        let sender = self.senders.get(id);
        if sender.is_none() {
            return Err(anyhow!("Unknown tunnel id"));
        }

        sender
            .unwrap()
            .send(bytes.to_vec())
            .await
            .map_err(|err| anyhow!("Unable to send: {err}"))
    }

    async fn handle_remote_dead(&mut self, routes: &[RouteId]) {
        for route_id in routes {
            for id in self.senders.clone().keys() {
                if id.0 == *route_id {
                    self.senders.remove(id);
                }
            }
        }
    }
    async fn handle_local_dead(&mut self, routes: &[RouteId]) {
        for route_id in routes {
            if *route_id != self.route_id {
                continue;
            }
            if let Some(callback) = &self.on_route_disconnected_callback {
                callback();
            }
            for id in self.senders.keys() {
                if id.0 == *route_id {
                    let _ = self.notify_bytes(id, &[]).await;
                }
            }
            // TODO: Better error handling?
            let (route_id, route_id_blob) = crate::make_route(&self.veilid).await.unwrap();
            self.route_id = route_id.clone();
            self.route_id_blob = route_id_blob.clone();
            if let Some(callback) = &self.on_new_route_callback {
                callback(route_id, route_id_blob);
            }
        }
        self.senders.clear();
    }
}

impl TunnelManager {
    async fn send_ping(&self, id: &TunnelId) -> Result<()> {
        let route_id_blob = {
            let inner = self.inner.lock().await;
            inner.route_id_blob.clone()
        };
        let mut bytes = PING_BYTES.to_vec();
        bytes.extend(route_id_blob);
        self.send_bytes(id, bytes).await
    }

    async fn send_bytes(&self, id: &TunnelId, bytes: Vec<u8>) -> Result<()> {
        let (router, route_id) = {
            let inner = self.inner.lock().await;
            (inner.router.clone(), inner.route_id.clone())
        };

        let route_id_bytes = Vec::from(route_id);
        let mut buffer: BytesMut = BytesMut::with_capacity(bytes.len() + 4 + route_id_bytes.len());
        buffer.put(route_id_bytes.as_slice());
        buffer.put_u32(id.1);
        buffer.put(bytes.as_slice());
        let target = Target::RouteId(id.0.clone());
        let result = router.app_call(target, buffer.to_vec()).await?;

        if result.len() != 1 {
            return Err(anyhow!(
                "Got invalid response length from app call: {result:?}"
            ));
        }

        let code: TunnelResult = result[0].try_into()?;

        match code {
            TunnelResult::Success => Ok(()),
            TunnelResult::Closed => Err(anyhow!("Tunnel closed")),
            TunnelResult::InvalidFormat => Err(anyhow!("Invalid Format")),
        }
    }

    fn new_tunnel_route_blob(message: &[u8]) -> Result<&[u8]> {
        if message.len() < PING_BYTES.len() {
            return Err(anyhow!(
                "Invalid new tunnel message length: got {}, expected at least {}",
                message.len(),
                PING_BYTES.len()
            ));
        }

        let ping = &message[..PING_BYTES.len()];
        if ping != PING_BYTES {
            return Err(anyhow!(
                "Invalid tunnel ping prefix: {ping:?}\n Expected: {PING_BYTES:?}"
            ));
        }

        Ok(&message[PING_BYTES.len()..])
    }

    async fn track(&self, id: &TunnelId) -> Result<Tunnel> {
        let (man_to_tun, from_man_to_tun) = mpsc::channel(100);
        let (tun_to_man, mut from_tun_to_man) = mpsc::channel::<Vec<u8>>(100);

        {
            let mut inner = self.inner.lock().await;

            inner.senders.insert(id.clone(), man_to_tun);
        }

        let manager = self.clone();
        let id = id.clone();
        let route_id = self.route_id().await;

        tokio::spawn(async move {
            while let Some(bytes) = from_tun_to_man.recv().await {
                if bytes.is_empty() {
                    // Signal that the tunnel is closed
                    break;
                }
                let result = manager.send_bytes(&id, bytes).await;
                if result.is_err() {
                    // TODO: report tunnel close somewhere? Should close one end once we break
                    eprint!("{0} Unable to read {1}", route_id, result.unwrap_err());
                    break;
                }
            }
        });

        Ok((tun_to_man, from_man_to_tun))
    }

    async fn handle_new(&self, id: &TunnelId, message: &[u8]) -> Result<()> {
        let route_id_blob = Self::new_tunnel_route_blob(message)?;

        let route_id = self
            .veilid
            .import_remote_private_route(route_id_blob.to_vec())?;

        if route_id != id.0 {
            return Err(anyhow!("Route ID and route blob don't match"));
        }

        let tunnel = self.track(id).await?;

        if self.on_new_tunnel.is_some() {
            self.on_new_tunnel.as_ref().unwrap()(tunnel);
        }

        Ok(())
    }

    async fn send_to_tunnel(&self, id: &TunnelId, bytes: &[u8]) -> Result<()> {
        let inner = self.inner.lock().await;
        inner.notify_bytes(id, bytes).await
    }

    async fn has_tunnel(&self, id: &TunnelId) -> bool {
        let inner = self.inner.lock().await;
        inner.senders.contains_key(id)
    }

    async fn handle_message(&self, id: &TunnelId, message: &[u8]) -> Result<(), TunnelError> {
        if self.has_tunnel(id).await {
            // TODO: Log failed requests?
            if let Err(err) = self.send_to_tunnel(id, message).await {
                let route_id = self.route_id().await;
                error!(route_id = ?route_id, error = ?err, "Unable to send data to tunnel");
                return Err(TunnelError::Delivery);
            }
        } else if let Err(err) = self.handle_new(id, message).await {
            let route_id = self.route_id().await;
            error!(route_id = ?route_id, error = ?err, "Unable to handle new tunnel");
            return Err(TunnelError::InvalidFrame);
        }

        Ok(())
    }

    async fn handle_app_call(&self, app_call: &VeilidAppCall) -> Result<()> {
        // No route or wrong route means it's prob from elsewhere
        if app_call.route_id().is_none() {
            return Ok(());
        }
        let route_id = app_call.route_id().unwrap();
        if route_id != &self.route_id().await {
            return Ok(());
        }

        let call_id = app_call.id();

        let mut buffer = Bytes::copy_from_slice(app_call.message());

        // Read route_id bytes (variable length based on crypto kind)
        let current_route_id = self.route_id().await;
        let route_id_len = Vec::from(current_route_id).len();
        let route_id_buffer = buffer.get(0..route_id_len);
        if route_id_buffer.is_none() {
            return self
                .app_call_reply(call_id, TunnelResult::InvalidFormat)
                .await;
        }
        let route_id_buffer = route_id_buffer.unwrap();
        let route_key = match RouteId::try_from(route_id_buffer.to_vec()) {
            Ok(route_key) => route_key,
            Err(err) => {
                error!(error = ?err, "Failed to parse tunnel route id");
                return self
                    .app_call_reply(call_id, TunnelResult::InvalidFormat)
                    .await;
            }
        };

        // Apparently .get(index) doesn't advance the buffer 🤷
        buffer.advance(route_id_len);

        if buffer.remaining() < std::mem::size_of::<u32>() {
            return self
                .app_call_reply(call_id, TunnelResult::InvalidFormat)
                .await;
        }

        let tunnel_number = buffer.get_u32();
        let bytes = buffer.chunk();

        let id: TunnelId = (route_key, tunnel_number);

        match self.handle_message(&id, bytes).await {
            Ok(_) => self.app_call_reply(call_id, TunnelResult::Success).await,
            Err(TunnelError::InvalidFrame) => {
                self.app_call_reply(call_id, TunnelResult::InvalidFormat)
                    .await
            }
            Err(TunnelError::Delivery) => self.app_call_reply(call_id, TunnelResult::Closed).await,
        }
    }

    async fn app_call_reply(&self, call_id: OperationId, result: TunnelResult) -> Result<()> {
        self.veilid
            .app_call_reply(call_id, vec![result as u8])
            .await?;
        Ok(())
    }

    async fn handle_remote_dead(&self, routes: &[RouteId]) {
        let mut inner = self.inner.lock().await;
        inner.handle_remote_dead(routes).await
    }

    async fn handle_local_dead(&self, routes: &[RouteId]) {
        let mut inner = self.inner.lock().await;
        inner.handle_local_dead(routes).await
    }

    pub async fn from_veilid(
        veilid: VeilidAPI,
        on_new_tunnel: Option<OnNewTunnelCallback>,
        on_route_disconnected_callback: Option<OnRouteDisconnectedCallback>,
        on_new_route_callback: Option<OnNewRouteCallback>,
    ) -> Result<Self> {
        let router = veilid.routing_context()?;
        let route_blob = veilid
            .new_custom_private_route(veilid_core::PrivateSpec {
                crypto_kinds: veilid_core::VALID_CRYPTO_KINDS.to_vec(),
                hop_count: 0,
                stability: veilid_core::Stability::LowLatency,
                sequencing: veilid_core::Sequencing::PreferUnordered,
            })
            .await?;
        let route_id = route_blob.route_id;
        let route_id_blob = route_blob.blob;

        Ok(Self::new(
            veilid,
            router,
            route_id,
            route_id_blob,
            on_new_tunnel,
            on_route_disconnected_callback,
            on_new_route_callback,
        ))
    }

    pub fn new(
        veilid: VeilidAPI,
        router: RoutingContext,
        route_id: RouteId,
        route_id_blob: Vec<u8>,
        on_new_tunnel: Option<OnNewTunnelCallback>,
        on_route_disconnected_callback: Option<OnRouteDisconnectedCallback>,
        on_new_route_callback: Option<OnNewRouteCallback>,
    ) -> Self {
        let inner = Arc::new(Mutex::new(TunnelManagerInner {
            route_id,
            route_id_blob: route_id_blob.clone(),
            router,
            veilid: veilid.clone(),
            senders: HashMap::new(),
            id_counter: 0,
            on_route_disconnected_callback,
            on_new_route_callback,
        }));

        TunnelManager {
            inner,
            veilid,
            on_new_tunnel,
        }
    }

    pub async fn route_id(&self) -> RouteId {
        let inner = self.inner.lock().await;

        inner.route_id.clone()
    }

    pub async fn route_id_blob(&self) -> Vec<u8> {
        let inner = self.inner.lock().await;

        inner.route_id_blob.clone()
    }

    pub async fn open(&self, route_id_blob: Vec<u8>) -> Result<Tunnel> {
        let route_id = self.veilid.import_remote_private_route(route_id_blob)?;
        let tunnel_id: u32;
        {
            let mut inner = self.inner.lock().await;
            inner.id_counter += 1;
            tunnel_id = inner.id_counter;
        }

        let id: TunnelId = (route_id, tunnel_id);

        let tunnel = self.track(&id).await?;

        self.send_ping(&id).await?;

        Ok(tunnel)
    }

    pub async fn listen(
        &self,
        mut updates: tokio::sync::broadcast::Receiver<VeilidUpdate>,
    ) -> Result<()> {
        while let Ok(update) = updates.recv().await {
            if let VeilidUpdate::AppCall(app_call) = update {
                if let Err(err) = self.handle_app_call(&app_call).await {
                    error!(error = ?err, "Error handling AppCall");
                }
            } else if let VeilidUpdate::RouteChange(route_change) = update {
                if !route_change.dead_remote_routes.is_empty() {
                    self.handle_remote_dead(&route_change.dead_remote_routes)
                        .await;
                }
                if !route_change.dead_routes.is_empty() {
                    self.handle_local_dead(&route_change.dead_routes).await;
                }
            }
            //println!("{0} Got event in manager");
        }

        Ok(())
    }

    pub async fn shutdown(self) -> Result<()> {
        // TODO: close routes and tunnels first?
        self.veilid.shutdown().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tunnel_route_blob_rejects_short_messages() {
        let err = TunnelManager::new_tunnel_route_blob(&[99])
            .expect_err("short new-tunnel messages should be rejected, not sliced");

        assert!(
            err.to_string()
                .contains("Invalid new tunnel message length"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn new_tunnel_route_blob_rejects_invalid_ping_prefix() {
        let err = TunnelManager::new_tunnel_route_blob(&[0, 0, 0, 0, 1, 2, 3])
            .expect_err("messages with the wrong ping prefix should be rejected");

        assert!(
            err.to_string().contains("Invalid tunnel ping prefix"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn new_tunnel_route_blob_returns_empty_slice_when_only_ping() {
        let route_blob = TunnelManager::new_tunnel_route_blob(PING_BYTES)
            .expect("message with only ping prefix should return empty slice");

        assert!(route_blob.is_empty());
    }

    #[test]
    fn new_tunnel_route_blob_returns_route_blob_after_ping() {
        let mut message = PING_BYTES.to_vec();
        message.extend([1, 2, 3]);

        let route_blob = TunnelManager::new_tunnel_route_blob(&message)
            .expect("valid ping prefix should return the remaining route blob");

        assert_eq!(route_blob, &[1, 2, 3]);
    }

    #[test]
    fn new_tunnel_route_blob_handles_large_payloads() {
        let mut message = PING_BYTES.to_vec();
        message.extend(vec![42u8; 1024]);

        let route_blob = TunnelManager::new_tunnel_route_blob(&message)
            .expect("large payloads should be parsed correctly");

        assert_eq!(route_blob.len(), 1024);
        assert!(route_blob.iter().all(|b| *b == 42u8));
    }
}
