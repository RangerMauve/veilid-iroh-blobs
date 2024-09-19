use crate::init_veilid;
use crate::make_route;
use crate::tunnels::TunnelManager;

use anyhow::anyhow;
use anyhow::Ok;
use anyhow::Result;
use async_stream::stream;
use bytes::Bytes;
use futures_lite::stream;
use futures_lite::stream::once;
use futures_lite::stream::unfold;
use futures_lite::{Stream, StreamExt};
use futures_util::lock::Mutex;
use futures_util::pin_mut;
use iroh_blobs::store::ImportMode;
use iroh_blobs::store::ImportProgress;
use iroh_blobs::store::Map;
use iroh_blobs::store::ReadableStore;
use iroh_blobs::store::Store;
use iroh_blobs::util::progress::IgnoreProgressSender;
use iroh_blobs::BlobFormat;
use iroh_blobs::Hash;
use iroh_io::AsyncSliceReader;
use iroh_io::AsyncSliceReaderExt;
use std::cmp::min;
use std::io::Error;
use std::io::ErrorKind;
use std::path::Path;
use std::result;
use std::str::FromStr;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tracing::info;
use veilid_core::{RouteId, RoutingContext, VeilidAPI, VeilidUpdate};

pub struct VeilidIrohBlobs {
    veilid: VeilidAPI,
    tunnels: TunnelManager,
    store: iroh_blobs::store::fs::Store,
}

impl VeilidIrohBlobs {
    pub fn new(
        veilid: VeilidAPI,
        router: RoutingContext,
        route_id_blob: Vec<u8>,
        route_id: RouteId,
        updates: Receiver<VeilidUpdate>,
        store: iroh_blobs::store::fs::Store,
    ) -> Self {
        let on_new_tunnel = Arc::new(|tunnel| {
            println!("{:?}", tunnel);
        });

        let tunnels = TunnelManager::new(
            veilid.clone(),
            router,
            route_id,
            route_id_blob,
            Some(on_new_tunnel),
        );

        let listening = tunnels.clone();
        tokio::spawn(async move {
            listening.listen(updates).await.unwrap();
        });
        VeilidIrohBlobs {
            veilid,
            store,
            tunnels,
        }
    }

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

    pub async fn upload_from_path(&self, file: PathBuf) -> Result<Hash> {
        let progress = IgnoreProgressSender::<ImportProgress>::default();
        let (tag, _) = self
            .store
            .import_file(file, ImportMode::Copy, BlobFormat::Raw, progress)
            .await?;

        let hash = tag.hash().clone();
        return Ok(hash);
    }

    pub async fn read_file(&self, hash: Hash) -> Result<mpsc::Receiver<std::io::Result<Bytes>>> {
        let chunk_size = 1024usize; // TODO: what's a good chunk size for veilid messages?

        let (send, read) = mpsc::channel::<std::io::Result<Bytes>>(2);

        let (reader, size) = self.open_file_reader(hash).await?;

        let reader = Arc::new(Mutex::new(reader));

        tokio::spawn(async move {
            let index = 0usize;
            loop {
                if index >= size {
                    return;
                }

                let remaining = size - (index as usize) - chunk_size;
                let chunk_size = min(chunk_size, remaining);
                {
                    let reader = reader.lock().await;
                    let chunk = reader.read_at(index as u64, chunk_size).await;

                    if send.send(chunk).await.is_err() {
                        return;
                    }
                }
            }
        });

        return Ok(read);
    }

    async fn open_file_reader(
        &self,
        hash: Hash,
    ) -> std::io::Result<(impl AsyncSliceReader, usize)> {
        let handle = self.store.get(&hash).await?;

        if !handle.is_some() {
            return std::io::Result::Err(Error::new(ErrorKind::Other, "Unable to get file"));
        }

        let mut reader = handle.unwrap().data_reader();
        let size = reader.size().await? as usize;
        return std::io::Result::Ok((reader, size));
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

    let stream = blobs.read_file(hash).await;

    blobs.veilid.shutdown().await;
}
