use crate::init_veilid;
use crate::make_route;
use crate::tunnels::OnNewTunnelCallback;
use crate::tunnels::Tunnel;
use crate::tunnels::TunnelManager;

use anyhow::anyhow;
use anyhow::Ok;
use anyhow::Result;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use futures_lite::{Stream, StreamExt};
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
use std::process::Command;
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
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

#[derive(Clone)]
pub struct VeilidIrohBlobs {
    tunnels: TunnelManager,
    store: iroh_blobs::store::fs::Store,
}

impl VeilidIrohBlobs {
    pub async fn from_directory(base_dir: &PathBuf, namespace: Option<String>) -> Result<Self> {
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
        ));
    }

    pub fn new(
        veilid: VeilidAPI,
        router: RoutingContext,
        route_id_blob: Vec<u8>,
        route_id: RouteId,
        updates: Receiver<VeilidUpdate>,
        store: iroh_blobs::store::fs::Store,
    ) -> Self {
        let (send_tunnel, read_tunnel) = mpsc::channel::<Tunnel>(1);

        let on_new_tunnel: OnNewTunnelCallback = Arc::new(move |tunnel| {
            let send_tunnel = send_tunnel.clone();
            tokio::spawn(async move {
                println!("New connection!");
                let _ = send_tunnel.send(tunnel).await;
            });
        });

        let tunnels =
            TunnelManager::new(veilid, router, route_id, route_id_blob, Some(on_new_tunnel));

        let listening = tunnels.clone();

        let blobs = VeilidIrohBlobs { store, tunnels };

        let listening_blobs = blobs.clone();

        tokio::spawn(async move {
            listening.listen(updates).await.unwrap();
        });
        tokio::spawn(async move {
            listening_blobs.listen(read_tunnel).await.unwrap();
        });

        return blobs;
    }

    pub async fn shutdown(self) -> Result<()> {
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

        if let Some(message) = read.recv().await {
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
                        while let Some(chunk) = file.recv().await {
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
        if let Some(result) = read.recv().await {
            println!("Got answer {:?}", result.clone());
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
        } else {
            return Err(anyhow!("Unable to ask peer"));
        }
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

        if let Some(result) = read.recv().await {
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
                    while let Some(message) = read.recv().await {
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
        } else {
            return Err(anyhow!("Unable to ask peer"));
        }
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

    pub fn route_id_blob(&self) -> Vec<u8> {
        return self.tunnels.route_id_blob();
    }
}

async fn init_deps(
    namespace: Option<String>,
    base_dir: &PathBuf,
) -> Result<(
    VeilidAPI,
    Receiver<VeilidUpdate>,
    iroh_blobs::store::fs::Store,
)> {
    let store = iroh_blobs::store::fs::Store::load(base_dir.join("iroh")).await?;

    let (veilid, rx) = init_veilid(namespace, base_dir).await?;

    return Ok((veilid, rx, store));
}

#[tokio::test]
async fn test_blobs() {
    //unsafe { backtrace_on_stack_overflow::enable() }

    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None)
        .await
        .unwrap();

    let hash = blobs
        .upload_from_path(std::fs::canonicalize(Path::new("./README.md").to_path_buf()).unwrap())
        .await
        .unwrap();

    println!("Hash of README: {0}", hash);

    let receiver = blobs.read_file(hash).await.unwrap();

    let mut stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

    let data = stream.next().await;

    println!("{:?}", data);

    let has = blobs.has_hash(&hash).await;

    println!("Blobs has hash: {}", has);

    blobs.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_blob_replication() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");
    let (veilid, mut rx) = init_veilid(None, &base_dir).await.unwrap();

    let (send_update, read_update) = broadcast::channel::<VeilidUpdate>(256);
    let read_update1 = read_update;
    let read_update2 = send_update.subscribe();

    let sender_handle = tokio::spawn(async move {
        while let Result::Ok(update) = rx.recv().await {
            if let VeilidUpdate::RouteChange(change) = update {
                println!("Route change {:?}", change);
                continue;
            }
            //println!("Received update: {:#?}", update);
            if let Err(err) = send_update.send(update) {
                eprintln!("Unable to process veilid update: {:?}", err);
            }
        }
    });

    let v1 = veilid.clone();
    let v2 = veilid.clone();

    let mut store1_dir = base_dir.clone();
    store1_dir.push("peer1");
    let store1 = iroh_blobs::store::fs::Store::load(store1_dir)
        .await
        .unwrap();

    let mut store2_dir = base_dir.clone();
    store2_dir.push("peer2");
    let store2 = iroh_blobs::store::fs::Store::load(store2_dir)
        .await
        .unwrap();
    let router1 = v1.routing_context().unwrap();
    let (route_id1, route_id1_blob) = make_route(&v1).await.unwrap();

    println!("Initializing route2");
    let router2 = v2.routing_context().unwrap();
    let (route_id2, route_id2_blob) = make_route(&v2).await.unwrap();

    let blobs1 = VeilidIrohBlobs::new(v1, router1, route_id1_blob, route_id1, read_update1, store1);
    let blobs2 = VeilidIrohBlobs::new(v2, router2, route_id2_blob, route_id2, read_update2, store2);

    let hash = blobs1
        .upload_from_path(std::fs::canonicalize(Path::new("./README.md").to_path_buf()).unwrap())
        .await
        .unwrap();

    blobs2
        .download_file_from(blobs1.route_id_blob(), &hash)
        .await
        .unwrap();

    let has = blobs2.has_hash(&hash).await;

    println!("Blobs has hash after download: {}", has);

    sender_handle.abort();
}
