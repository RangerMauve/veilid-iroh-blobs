use anyhow::{anyhow, Result};
use iroh_blobs::store::fs::Store;
use rdefer::defer;
use std::{path::PathBuf, sync::Arc};
use tmpdir::TmpDir;
use tokio::join;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::info;
use veilid_core::{
    RouteId, RoutingContext, UpdateCallback, VeilidAPI, VeilidConfigInner, VeilidUpdate,
};

mod tunnels;

use crate::tunnels::TunnelManager;

struct VeilidIrohBlobs {
    tunnels: TunnelManager,
    store: Store,
}

impl VeilidIrohBlobs {
    pub fn new(veilid: VeilidAPI, router: RoutingContext, route_id: RouteId, store: Store) -> Self {
        let on_new_tunnel = Arc::new(|tunnel| {
            println!("{:?}", tunnel);
        });

        let tunnels = TunnelManager::new(veilid, router, route_id, Some(on_new_tunnel));
        VeilidIrohBlobs { store, tunnels }
    }

    /*
    pub fn stream_blob(hash: Hash) -> impl Stream<Item = io::Result<Bytes>> + 'static {
        let request = GetRequest::single(hash);
    }
     */
}

/*
#[tokio::test]
async fn basic_test() {
    let tmp = TmpDir::new("test_dweb_backend").await.unwrap();
    let base_dir = tmp.to_path_buf();

    let dir1 = base_dir.join("p1");
    let dir2 = base_dir.join("p2");

    // Initialize Veilid
    let update_callback: UpdateCallback = Arc::new(|update| {
        info!("Received update: {:?}", update);
    });

    let (veilid, store, router, route_id) = init_deps(&base_dir, update_callback)
        .await
        .expect("Unable to init veilid and store");

    let blobs = VeilidIrohBlobs::new(router, route_id, store);

    veilid.shutdown().await;
    tmp.close().await;
}
 */

#[tokio::test]
async fn test_tunnel() {
    let tmp = TmpDir::new("test_dweb_backend").await.unwrap();
    let base_dir = tmp.to_path_buf();

    /* four threads
    veilid cb -> updates channel
    tunnel1, wait for tunnel to come in and send an OK to output channel, spawn thread for listening to updates
    tunnel2, open tunnel, wait for write, send OK to output channel, spawn thread for waiting for updates
    test, listen for two Results and unwrap them
        */

    let (send_update, mut read_update) = broadcast::channel::<VeilidUpdate>(256);
    let (send_result, mut read_result) = mpsc::channel::<Result<()>>(2);

    let read_update1 = read_update;
    let read_update2 = send_update.subscribe();

    let send_result1 = send_result.clone();
    let send_result2 = send_result.clone();

    // Initialize Veilid
    let update_callback: UpdateCallback = Arc::new(move |update| {
        //info!("Received update: {:?}", update);
        if let Err(err) = send_update.send(update) {
            eprintln!("Unable to process veilid update");
        }
    });

    let veilid = init_veilid(&base_dir, update_callback)
        .await
        .expect("Unable to init veilid and store");

    let v1 = veilid.clone();
    let v2 = veilid.clone();

    println!("Initializing route1");

    let router1 = veilid.routing_context().unwrap();
    let (route_id1, _) = veilid.new_private_route().await.unwrap();

    println!("Initializing route2");

    let router2 = veilid.routing_context().unwrap();
    let (route_id2, _) = veilid.new_private_route().await.unwrap();

    println!("Routes ready");

    let on_new_tunnel1 = Arc::new(move |tunnel| {
        println!("{:?}", tunnel);

        let send_result1 = send_result1.clone();

        tokio::spawn(async move {
            send_result1.send(Ok(())).await.unwrap();
        });
    });

    println!("Spawn tunnel1");
    let tunnel1_handle = tokio::spawn(async move {
        let tunnels1 = TunnelManager::new(v1, router1, route_id1, Some(on_new_tunnel1));

        tunnels1.listen(read_update1).await.unwrap();
    });
    println!("Spawn tunnel2");

    let tunnel2_handle = tokio::spawn(async move {
        let tunnels2 = TunnelManager::new(v2, router2, route_id2, None);
        let listening = tunnels2.clone();
        tokio::spawn(async move {
            println!("Listening in tunnel2");
            listening.listen(read_update2).await.unwrap();
        });

        let result = tunnels2.open(route_id1).await;
        send_result2
            .send(if result.is_ok() {
                Ok(())
            } else {
                Err(result.unwrap_err())
            })
            .await
            .unwrap();
    });

    print!("wait result1");

    read_result.recv().await.unwrap().unwrap();

    print!("wait result2");
    read_result.recv().await.unwrap().unwrap();

    tunnel1_handle.abort();
    tunnel2_handle.abort();

    print!("cleanup");
    veilid.shutdown().await;

    tmp.close().await.unwrap();
}

async fn init_veilid(base_dir: &PathBuf, update_callback: UpdateCallback) -> Result<VeilidAPI> {
    let config_inner = config_for_dir(base_dir.to_path_buf());

    let (tx, mut rx) = mpsc::channel(1);

    let mut isDone = false;

    let update_callback_local: UpdateCallback = Arc::new(move |update| {
        //info!("Received update: {:?}", update);
        if isDone {
            return;
        }

        let tx = tx.clone();

        tokio::spawn(async move {
            // TODO: 🤷
            if let Err(_) = tx.clone().send(update).await {
                isDone = true;
            }
        });
    });

    println!("Init veilid");
    let veilid = veilid_core::api_startup_config(update_callback_local, config_inner).await?;

    println!("Attach veilid");

    veilid.attach().await?;

    println!("Wait for veilid network");

    while let Some(update) = rx.recv().await {
        if let VeilidUpdate::Attachment(attachment_state) = update {
            if attachment_state.public_internet_ready {
                println!("Public internet ready!");
                break;
            }
        }
    }

    println!("Network ready");

    tokio::spawn(async move {
        while let Some(update) = rx.recv().await {
            update_callback(update)
        }
    });

    return Ok(veilid);
}

async fn init_deps(
    base_dir: &PathBuf,
    update_callback: UpdateCallback,
) -> Result<(VeilidAPI, Store)> {
    let store = Store::load(base_dir.join("iroh")).await?;

    let veilid = init_veilid(base_dir, update_callback).await?;

    return Ok((veilid, store));
}

fn config_for_dir(base_dir: PathBuf) -> VeilidConfigInner {
    return VeilidConfigInner {
        program_name: "iroh-blobs".to_string(),
        namespace: "iroh".into(),
        capabilities: Default::default(),
        protected_store: veilid_core::VeilidConfigProtectedStore {
            // avoid prompting for password, don't do this in production
            allow_insecure_fallback: true,
            always_use_insecure_storage: true,
            directory: base_dir
                .join("protected_store")
                .to_string_lossy()
                .to_string(),
            delete: false,
            device_encryption_key_password: "".to_string(),
            new_device_encryption_key_password: None,
        },
        table_store: veilid_core::VeilidConfigTableStore {
            directory: base_dir.join("table_store").to_string_lossy().to_string(),
            delete: false,
        },
        block_store: veilid_core::VeilidConfigBlockStore {
            directory: base_dir.join("block_store").to_string_lossy().to_string(),
            delete: false,
        },
        network: Default::default(),
    };
}

fn main() {
    println!("Hello, world!");
}
