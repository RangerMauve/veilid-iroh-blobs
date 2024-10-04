use crate::init_deps;
use crate::init_veilid;
use crate::make_route;
use crate::tunnels::OnNewRouteCallback;
use crate::tunnels::OnNewTunnelCallback;
use crate::tunnels::OnRouteDisconnectedCallback;
use crate::tunnels::Tunnel;
use crate::tunnels::TunnelManager;

use anyhow::anyhow;
use anyhow::Ok;
use anyhow::Result;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use iroh_blobs::store::ImportMode;
use iroh_blobs::store::ImportProgress;
use iroh_blobs::store::Map;
use iroh_blobs::store::ReadableStore;
use iroh_blobs::store::Store;
use iroh_blobs::util::progress::IgnoreProgressSender;
use iroh_blobs::BlobFormat;
use iroh_blobs::Hash;
use iroh_io::AsyncSliceReader;
use std::io::ErrorKind;
use std::path::Path;
use std::result;
use std::sync::Mutex;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use veilid_core::{RouteId, RoutingContext, VeilidAPI, VeilidUpdate};

const NO: u8 = 0x00u8;
const YES: u8 = 0x01u8;
const HAS: u8 = 0x10u8;
const ASK: u8 = 0x11u8;
const DATA: u8 = 0x20u8;
const DONE: u8 = 0x22u8;
const ERR: u8 = 0xF0u8;

const DEFAULT_TIMEOUT: Duration = Duration::from_millis(4000);

#[derive(Clone)]
pub struct VeilidIrohBlobs {
    tunnels: TunnelManager,
    store: iroh_blobs::store::fs::Store,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl VeilidIrohBlobs {
    pub async fn from_directory(
        base_dir: &PathBuf,
        namespace: Option<String>,
        on_route_disconnected_callback: Option<OnRouteDisconnectedCallback>,
        on_new_route_callback: Option<OnNewRouteCallback>,
    ) -> Result<Self> {
        let (veilid, updates, store) = init_deps(namespace, base_dir).await?;

        let router = veilid.routing_context().unwrap();
        let (route_id, route_id_blob) = make_route(&veilid).await.unwrap();

        return Ok(Self::new(
            veilid,
            router,
            route_id_blob,
            route_id,
            updates,
            store,
            on_route_disconnected_callback,
            on_new_route_callback,
        ));
    }

    pub fn new(
        veilid: VeilidAPI,
        router: RoutingContext,
        route_id_blob: Vec<u8>,
        route_id: RouteId,
        updates: Receiver<VeilidUpdate>,
        store: iroh_blobs::store::fs::Store,
        on_route_disconnected_callback: Option<OnRouteDisconnectedCallback>,
        on_new_route_callback: Option<OnNewRouteCallback>,
    ) -> Self {
        let (send_tunnel, read_tunnel) = mpsc::channel::<Tunnel>(1);

        let on_new_tunnel: OnNewTunnelCallback = Arc::new(move |tunnel| {
            let send_tunnel = send_tunnel.clone();
            tokio::spawn(async move {
                println!("New connection!");
                let _ = send_tunnel.send(tunnel).await;
            });
        });

        let tunnels = TunnelManager::new(
            veilid,
            router,
            route_id,
            route_id_blob,
            Some(on_new_tunnel),
            on_route_disconnected_callback,
            on_new_route_callback,
        );

        let listening = tunnels.clone();

        let handles = Arc::new(Mutex::new(Vec::with_capacity(2)));

        let blobs = VeilidIrohBlobs {
            store,
            tunnels,
            handles: handles.clone(),
        };

        let listening_blobs = blobs.clone();

        let tunnels_handle = tokio::spawn(async move {
            listening.listen(updates).await.unwrap();
        });
        let blobs_handle = tokio::spawn(async move {
            listening_blobs.listen(read_tunnel).await.unwrap();
        });

        let mut handles = handles.lock().unwrap();
        handles.push(tunnels_handle);
        handles.push(blobs_handle);

        return blobs;
    }

    pub async fn shutdown(self) -> Result<()> {
        let handles = self.handles.lock().unwrap();
        handles[0].abort();
        handles[1].abort();
        self.tunnels.shutdown().await?;
        self.store.shutdown().await;
        return Ok(());
    }
    async fn listen(&self, mut on_new_tunnel: mpsc::Receiver<Tunnel>) -> Result<()> {
        while let Some(tunnel) = on_new_tunnel.recv().await {
            let self_clone = self.clone();
            tokio::spawn(async move {
                self_clone.handle_tunnel(tunnel).await;
            });
        }
        return Ok(());
    }

    async fn handle_tunnel(&self, tunnel: Tunnel) {
        let (send, mut read) = tunnel;

        let read_result = timeout(DEFAULT_TIMEOUT, read.recv()).await;

        if !read_result.is_ok() {
            // Tunnel likely closed
            // TODO: log error?
            return;
        }

        if let Some(message) = read_result.unwrap() {
            let command = message[0];
            let hash_bytes = &message[1..];
            if command == ASK || command == HAS {
                if hash_bytes.len() != 32 {
                    eprintln!("Got invalid hash bytes length {}", hash_bytes.len());
                    let _ = send.send(vec![ERR]).await;
                    return;
                }
                let bytes: [u8; 32] = hash_bytes.try_into().unwrap();
                let hash = Hash::from_bytes(bytes);
                let has = self.has_hash(&hash).await;
                if has {
                    let _ = send.send(vec![YES]).await;
                } else {
                    let _ = send.send(vec![NO]).await;
                    return;
                }
                if command == ASK {
                    if let Result::Ok(mut file) = self.read_file(hash).await {
                        while let Result::Ok(read_result) =
                            timeout(DEFAULT_TIMEOUT, file.recv()).await
                        {
                            if !read_result.is_some() {
                                break;
                            }
                            let chunk = read_result.unwrap();
                            if chunk.is_err() {
                                let _ = send.send(vec![ERR]).await;
                                return;
                            } else {
                                let chunk = chunk.unwrap();
                                let mut to_send = BytesMut::with_capacity(chunk.len() + 1);
                                to_send.put_u8(DATA);
                                to_send.put(chunk);

                                if let Err(_) = send.send(to_send.to_vec()).await {
                                    return;
                                }
                            }
                        }
                        let _ = send.send(vec![DONE]).await;
                    } else {
                        let _ = send.send(vec![ERR]).await;
                    }
                }
            } else {
                let _ = send.send(vec![ERR]).await;
            }
        }
    }

    pub async fn has_hash(&self, hash: &Hash) -> bool {
        if let std::io::Result::Ok(entry) = self.store.get(hash).await {
            return entry.is_some();
        } else {
            return false;
        }
    }

    pub async fn ask_hash(&self, route_id_blob: Vec<u8>, hash: Hash) -> Result<bool> {
        let tunnel = self.tunnels.open(route_id_blob).await?;
        let hash_bytes = hash.as_bytes();
        let mut to_send = BytesMut::with_capacity(hash_bytes.len() + 1);
        to_send.put_u8(HAS);
        to_send.put(hash_bytes.as_slice());

        let (send, mut read) = tunnel;

        send.send(to_send.to_vec()).await?;

        println!("Sent HAS, waiting for response");
        if let Result::Ok(read_result) = timeout(DEFAULT_TIMEOUT, read.recv()).await {
            if let Some(result) = read_result {
                if result.len() != 1 {
                    return Err(anyhow!(
                        "Invalid response length from peer {}",
                        result.len()
                    ));
                }

                let command = result[0];
                if command == YES {
                    return Ok(true);
                } else if command == NO {
                    return Ok(false);
                } else {
                    return Err(anyhow!("Invalid response code from peer {:?}", command));
                }
            }
        }

        return Err(anyhow!("Unable to ask peer"));
    }

    pub async fn download_file_from(&self, route_id_blob: Vec<u8>, hash: &Hash) -> Result<()> {
        let tunnel = self.tunnels.open(route_id_blob).await?;
        let hash_bytes = hash.as_bytes();
        let mut to_send = BytesMut::with_capacity(hash_bytes.len() + 1);
        to_send.put_u8(ASK);
        to_send.put(hash_bytes.as_slice());

        let (send, mut read) = tunnel;

        send.send(to_send.to_vec()).await?;

        println!("Sent ask, waiting for answer");

        if let Result::Ok(read_result) = timeout(DEFAULT_TIMEOUT, read.recv()).await {
            if let Some(result) = read_result {
                println!("Got response");
                if result.len() != 1 {
                    return Err(anyhow!(
                        "Invalid response length from peer {}",
                        result.len()
                    ));
                }

                let command = result[0];
                if command == YES {
                    let (send_file, read_file) = mpsc::channel::<std::io::Result<Bytes>>(2);

                    tokio::spawn(async move {
                        while let Result::Ok(read_result) =
                            timeout(DEFAULT_TIMEOUT, read.recv()).await
                        {
                            if read_result.is_none() {
                                break;
                            }
                            let message = read_result.unwrap();

                            if message.len() < 1 {
                                let _ = send_file
                                    .send(std::io::Result::Err(std::io::Error::new(
                                        ErrorKind::InvalidData,
                                        "Peer sent empty message",
                                    )))
                                    .await;
                                return;
                            }
                            let command = message[0];

                            if command == DONE {
                                break;
                            }

                            if command != DATA {
                                let _ = send_file
                                    .send(std::io::Result::Err(std::io::Error::new(
                                        ErrorKind::InvalidData,
                                        format!("Peer sent unexpected command {}", command),
                                    )))
                                    .await;
                                return;
                            }
                            let bytes = Bytes::from_iter(message[1..].to_vec());
                            if let Err(_) = send_file.send(std::io::Result::Ok(bytes)).await {
                                return;
                            }
                        }
                    });
                    let got_hash = self.upload_from_stream(read_file).await?;

                    if got_hash.eq(hash) {
                        return Ok(());
                    } else {
                        self.store.delete(vec![got_hash]).await?;
                        return Err(anyhow!("Peer returned invalid hash {}", got_hash));
                    }
                } else if command == NO {
                    return Err(anyhow!("Peer does not have hash"));
                } else {
                    return Err(anyhow!("Invalid response code from peer {:?}", command));
                }
            }
        }
        return Err(anyhow!("Unable to ask peer"));
    }

    pub async fn upload_from_path(&self, file: PathBuf) -> Result<Hash> {
        let progress = IgnoreProgressSender::<ImportProgress>::default();
        let (tag, _) = self
            .store
            .import_file(file, ImportMode::Copy, BlobFormat::Raw, progress)
            .await?;

        let hash = tag.hash().clone();
        return Ok(hash);
    }

    pub async fn upload_from_stream(
        &self,
        receiver: mpsc::Receiver<std::io::Result<Bytes>>,
    ) -> Result<Hash> {
        let stream = ReceiverStream::new(receiver);
        let progress = IgnoreProgressSender::<ImportProgress>::default();
        let (tag, _) = self
            .store
            .import_stream(stream, BlobFormat::Raw, progress)
            .await?;

        let hash = tag.hash().clone();
        return Ok(hash);
    }

    pub async fn read_file(&self, hash: Hash) -> Result<mpsc::Receiver<std::io::Result<Bytes>>> {
        let handle = self.store.get(&hash).await?;

        if !handle.is_some() {
            return Err(anyhow!("Unable to find hash"));
        }

        let mut reader = handle.unwrap().data_reader();
        let size = reader.size().await? as usize;

        let chunk_size = 1024usize; // TODO: what's a good chunk size for veilid messages?

        let (send, read) = mpsc::channel::<std::io::Result<Bytes>>(2);

        tokio::spawn(async move {
            let mut index = 0usize;
            while index < size {
                let chunk = reader.read_at(index as u64, chunk_size).await;

                if let Err(err) = send.send(chunk).await {
                    eprintln!("Cannot send down channel {:?}", err);
                    return;
                }
                index += chunk_size
            }
        });

        return Ok(read);
    }

    pub async fn route_id_blob(&self) -> Vec<u8> {
        return self.tunnels.route_id_blob().await;
    }
}
