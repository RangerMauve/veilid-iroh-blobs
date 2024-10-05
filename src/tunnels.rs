use anyhow::{anyhow, Result};
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use veilid_core::CryptoKey;
use veilid_core::VeilidAPI;
use veilid_core::VeilidAppMessage;
use veilid_core::VALID_CRYPTO_KINDS;
use veilid_core::{RouteId, RoutingContext, Target, VeilidUpdate, CRYPTO_KEY_LENGTH};

pub type Tunnel = (Sender<Vec<u8>>, Receiver<Vec<u8>>);
pub type TunnelId = (RouteId, u32);
pub type OnNewTunnelCallback = Arc<dyn Fn(Tunnel) + Send + Sync>;
pub type OnRouteDisconnectedCallback = Arc<dyn Fn() + Send + Sync>;
pub type OnNewRouteCallback = Arc<dyn Fn(RouteId, Vec<u8>) + Send + Sync>;

// SAVE on a phone pad
static PING_BYTES: &[u8] = &[7, 2, 8, 3];

#[repr(u8)]
pub enum TunnelResult {
    Success = 0,
    InvalidFormat = 1,
    Closed = 2,
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
    async fn send_ping(&self, id: &TunnelId) -> Result<()> {
        let mut bytes = PING_BYTES.to_vec();
        bytes.extend(self.route_id_blob.clone());
        self.send_bytes(id, bytes).await
    }

    async fn send_bytes(&self, id: &TunnelId, bytes: Vec<u8>) -> Result<()> {
        let mut buffer: BytesMut = BytesMut::with_capacity(bytes.len() + 4 + CRYPTO_KEY_LENGTH);
        buffer.put(self.route_id.bytes.to_vec().as_slice());
        buffer.put_u32(id.1);
        buffer.put(bytes.as_slice());
        let target = Target::PrivateRoute(id.0);
        let result = self.router.app_message(target, buffer.to_vec()).await;

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!("{}", err)),
        }
    }

    async fn notify_bytes(&self, id: &TunnelId, bytes: &[u8]) -> Result<()> {
        let sender = self.senders.get(id);
        if sender.is_none() {
            return Err(anyhow!("Unknown tunnel id"));
        }

        sender
            .unwrap()
            .send(bytes.to_vec())
            .await
            .map_err(|err| anyhow!("Unable to send: {}", err))
    }

    async fn handle_remote_dead(&mut self, routes: &[CryptoKey]) {
        for route_id in routes {
            for id in self.senders.clone().keys() {
                if id.0 == *route_id {
                    self.senders.remove(id);
                }
            }
        }
    }
    async fn handle_local_dead(&mut self, routes: &[CryptoKey]) {
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
            let (route_id, route_id_blob) = make_route(&self.veilid).await.unwrap();
            self.route_id = route_id;
            self.route_id_blob = route_id_blob.clone();
            if let Some(callback) = &self.on_new_route_callback {
                callback(route_id, route_id_blob);
            }
        }
        self.senders.clear();
    }
}

impl TunnelManager {
    async fn track(&self, id: &TunnelId) -> Result<Tunnel> {
        let (man_to_tun, from_man_to_tun) = mpsc::channel(100);
        let (tun_to_man, mut from_tun_to_man) = mpsc::channel::<Vec<u8>>(100);

        {
            let mut inner = self.inner.lock().await;

            inner.senders.insert(*id, man_to_tun);
        }

        let inner = self.inner.clone();
        let id = *id;
        let route_id = self.route_id().await;

        tokio::spawn(async move {
            while let Some(bytes) = from_tun_to_man.recv().await {
                if bytes.is_empty() {
                    // Signal that the tunnel is closed
                    break;
                }
                let inner = inner.lock().await;
                let result = inner.send_bytes(&id, bytes).await;
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
        let ping = &message[0..PING_BYTES.len()];
        if !ping.eq(PING_BYTES) {
            return Err(anyhow!(
                "Got invalid length for ping: {:?}\n Expected: {:?}",
                ping,
                PING_BYTES
            ));
        }

        let route_id_blob = &message[PING_BYTES.len()..];

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

    async fn handle_message(&self, id: &TunnelId, message: &[u8]) -> Result<()> {
        if self.has_tunnel(id).await {
            // TODO: Log failed requests?
            if let Err(err) = self.send_to_tunnel(id, message).await {
                eprintln!(
                    "{0} Unable to send data to tunnel {1:?}",
                    self.route_id().await,
                    err
                );
            }
        } else if let Err(err) = self.handle_new(id, message).await {
            eprintln!(
                "{0} Unable to handle new tunnel {1:?}",
                self.route_id().await,
                err
            );
        }

        Ok(())
    }

    async fn handle_app_message(&self, app_messsage: &Box<VeilidAppMessage>) -> Result<()> {
        // No route or wrong route means it's prob from elsewhere
        if app_messsage.route_id().is_none() {
            return Ok(());
        }
        let route_id = app_messsage.route_id().unwrap();
        if route_id != &self.route_id().await {
            return Ok(());
        }

        let mut buffer = Bytes::copy_from_slice(app_messsage.message());

        // THis is all to read 32 bytes into a fixed buffer 💀
        let route_id_buffer = buffer.get(0..32);
        if route_id_buffer.is_none() {
            return Ok(());
        }
        let route_id_buffer = route_id_buffer.unwrap();
        let mut route_key_raw: [u8; CRYPTO_KEY_LENGTH] = [0; CRYPTO_KEY_LENGTH];
        route_key_raw.writer().write(route_id_buffer)?;
        let route_key = CryptoKey::from(route_key_raw);

        // Apparently .get(index) doesn't advance the buffer 🤷
        buffer.advance(32);

        let tunnel_number = buffer.get_u32();
        let bytes = buffer.chunk();

        let id: TunnelId = (route_key, tunnel_number);

        self.handle_message(&id, bytes).await?;

        Ok(())
    }

    async fn handle_remote_dead(&self, routes: &[CryptoKey]) {
        let mut inner = self.inner.lock().await;
        inner.handle_remote_dead(routes).await
    }

    async fn handle_local_dead(&self, routes: &[CryptoKey]) {
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
        let (route_id, route_id_blob) = veilid.new_private_route().await?;

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

        inner.route_id
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

        {
            let inner = self.inner.lock().await;
            inner.send_ping(&id).await?;
        }

        Ok(tunnel)
    }

    pub async fn listen(
        &self,
        mut updates: tokio::sync::broadcast::Receiver<VeilidUpdate>,
    ) -> Result<()> {
        while let Ok(update) = updates.recv().await {
            if let VeilidUpdate::AppMessage(app_message) = update {
                self.handle_app_message(&app_message).await?;
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

// TODO: Put these into a utils module or something
async fn make_route(veilid: &VeilidAPI) -> Result<(RouteId, Vec<u8>)> {
    let mut retries = 3;
    while retries != 0 {
        retries -= 1;
        let result = veilid
            .new_custom_private_route(
                &VALID_CRYPTO_KINDS,
                veilid_core::Stability::Reliable,
                veilid_core::Sequencing::EnsureOrdered,
            )
            .await;

        if let Ok(route) = result {
            return Ok(route);
        }
    }
    Err(anyhow!("Unable to create route, reached max retries"))
}
