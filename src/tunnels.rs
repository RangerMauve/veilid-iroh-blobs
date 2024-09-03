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
use veilid_core::OperationId;
use veilid_core::VeilidAPI;
use veilid_core::VeilidAppCall;
use veilid_core::VeilidAppMessage;
use veilid_core::{RouteId, RoutingContext, Target, VeilidUpdate, CRYPTO_KEY_LENGTH};

pub type Tunnel = (Sender<Vec<u8>>, Receiver<Vec<u8>>);
pub type TunnelId = (RouteId, u32);
pub type OnNewTunnelCallback = Arc<dyn Fn(Tunnel) + Send + Sync>;

// SAVE on a phone pad
static PING_BYTES: &'static [u8] = &[7, 2, 8, 3];

#[repr(u8)]
pub enum TunnelResult {
    Success = 0,
    InvalidFormat = 1,
    Closed = 2,
}

struct TunnelManagerInner {
    router: RoutingContext,
    route_id: RouteId,
    id_counter: u32,
    senders: HashMap<TunnelId, Sender<Vec<u8>>>,
}

#[derive(Clone)]
pub struct TunnelManager {
    inner: Arc<Mutex<TunnelManagerInner>>,
    route_id: RouteId,
    veilid: VeilidAPI,
    on_new_tunnel: Option<OnNewTunnelCallback>,
}

impl TunnelManagerInner {
    async fn send_ping(&self, id: &TunnelId) -> Result<()> {
        let bytes = self.route_id.bytes.to_vec();

        return self.send_bytes(id, bytes).await;
    }

    async fn send_bytes(&self, id: &TunnelId, bytes: Vec<u8>) -> Result<()> {
        // TODO: Don't unwrap
        println!("sending bytes");
        let mut buffer: BytesMut = BytesMut::with_capacity(bytes.len() + 4 + CRYPTO_KEY_LENGTH);
        buffer.put(id.0.bytes.to_vec().as_slice());
        buffer.put_u32(id.1);
        buffer.put(bytes.as_slice());
        let target = Target::PrivateRoute(id.0);
        let result = self.router.app_message(target, buffer.to_vec()).await;

        println!("sent bytes");

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!("{}", err)),
        }
    }

    async fn notify_bytes(&self, id: &TunnelId, bytes: &[u8]) -> Result<()> {
        let sender = self.senders.get(id);

        if !sender.is_some() {
            return Err(anyhow!("Unknown tunnel id"));
        }

        return sender
            .unwrap()
            .send(bytes.to_vec())
            .await
            .map_err(|err| anyhow!("Unable to send: {}", err));
    }
}

impl TunnelManager {
    async fn track(&self, id: &TunnelId) -> Result<Tunnel> {
        let (man_to_tun, from_man_to_tun) = mpsc::channel(100);
        let (tun_to_man, mut from_tun_to_man) = mpsc::channel(100);

        {
            let mut inner = self.inner.lock().await;

            inner.senders.insert(*id, man_to_tun);
        }

        let inner = self.inner.clone();
        let id = id.clone();

        tokio::spawn(async move {
            while let Some(bytes) = from_tun_to_man.recv().await {
                let inner = inner.lock().await;
                let result = inner.send_bytes(&id, bytes).await;
                if result.is_err() {
                    // TODO: report tunnel close somewhere? Should close one end once we break
                    eprint!("{}", result.unwrap_err());
                    break;
                }
            }
        });

        return Ok((tun_to_man, from_man_to_tun));
    }

    async fn reply(&self, call_id: OperationId, message: Vec<u8>) -> Result<()> {
        println!("sending reply {call_id}");
        return self
            .veilid
            .app_call_reply(call_id, message)
            .await
            .map_err(|err| anyhow!("{}", err));
    }

    async fn reply_result(&self, call_id: OperationId, result: TunnelResult) -> Result<()> {
        return self.reply(call_id, vec![result as u8]).await;
    }

    async fn handle_new(&self, id: &TunnelId, message: &[u8]) -> Result<()> {
        println!("handling new");
        if !message.eq(PING_BYTES) {
            return Err(anyhow!(
                "Got invalid length for ping: {:?}\n Expected: {:?}",
                message,
                PING_BYTES
            ));
        }

        let tunnel = self.track(id).await?;

        if self.on_new_tunnel.is_some() {
            println!("calling on_new callback");
            self.on_new_tunnel.as_ref().unwrap()(tunnel);
        }

        return Ok(());
    }

    async fn send_to_tunnel(&self, id: &TunnelId, bytes: &[u8]) -> Result<()> {
        let inner = self.inner.lock().await;
        return inner.notify_bytes(id, bytes).await;
    }

    async fn has_tunnel(&self, id: &TunnelId) -> bool {
        let inner = self.inner.lock().await;
        return inner.senders.contains_key(id);
    }

    async fn handle_message(&self, id: &TunnelId, message: &[u8]) -> Result<()> {
        println!("handling incoming message");
        if self.has_tunnel(id).await {
            println!("existing tunnel");

            // TODO: Log failed requests?
            if let Err(err) = self.send_to_tunnel(id, message).await {
                eprintln!("Unable to send data to tunnel {:?}", err);
            };
        }

        if let Err(err) = self.handle_new(id, message).await {
            eprintln!("Unable to handle new tunnel {:?}", err);
        };

        return Ok(());
    }

    async fn handle_app_call(&self, app_messsage: &Box<VeilidAppMessage>) -> Result<()> {
        // No route or wrong route means it's prob from elsewhere
        if !app_messsage.route_id().is_some() {
            println!("app call without route");
            return Ok(());
        }
        let route_id = app_messsage.route_id().unwrap();
        if route_id != &self.route_id {
            println!("app call for other route");
            return Ok(());
        }

        print!("handling app call");

        let mut buffer = Bytes::copy_from_slice(app_messsage.message());

        // THis is all to read 32 bytes into a fixed buffer 💀
        let route_id_buffer = buffer.get(0..32);
        if route_id_buffer.is_none() {
            println!("Missing route id buffer in app call");
            return Ok(());
        }
        let route_id_buffer = route_id_buffer.unwrap();
        let mut route_key_raw: [u8; CRYPTO_KEY_LENGTH] = [0; CRYPTO_KEY_LENGTH];
        route_key_raw.writer().write(route_id_buffer)?;
        let route_key = CryptoKey::from(route_key_raw);

        let tunnel_number = buffer.get_u32();
        let bytes = buffer.chunk();

        let id: TunnelId = (route_key, tunnel_number);

        self.handle_message(&id, bytes).await?;

        return Ok(());
    }

    pub async fn from_veilid(
        veilid: VeilidAPI,
        on_new_tunnel: Option<OnNewTunnelCallback>,
    ) -> Result<Self> {
        let router = veilid.routing_context()?;
        let (route_id, _) = veilid.new_private_route().await?;

        return Ok(Self::new(veilid, router, route_id, on_new_tunnel));
    }

    pub fn new(
        veilid: VeilidAPI,
        router: RoutingContext,
        route_id: RouteId,
        on_new_tunnel: Option<OnNewTunnelCallback>,
    ) -> Self {
        let inner = Arc::new(Mutex::new(TunnelManagerInner {
            route_id,
            router,
            senders: HashMap::new(),
            id_counter: 0,
        }));

        let tunnels = TunnelManager {
            route_id,
            inner,
            veilid,
            on_new_tunnel,
        };

        return tunnels;
    }

    pub fn route_id(&self) -> RouteId {
        return self.route_id;
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

        return Ok(tunnel);
    }

    pub async fn listen(
        &self,
        mut updates: tokio::sync::broadcast::Receiver<VeilidUpdate>,
    ) -> Result<()> {
        while let Ok(update) = updates.recv().await {
            if let VeilidUpdate::AppMessage(app_message) = update {
                println!("got appcall in manager");
                self.handle_app_call(&app_message).await?;
            }
            //println!("Got event in manager");
        }

        println!("FInished listening to updates");

        return Ok(());
    }
}

/*
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
 */
