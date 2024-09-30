use anyhow::anyhow;
use anyhow::Result;
use core::str;
use std::result;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tunnels::OnNewTunnelCallback;
// use iroh_blobs::store::fs::Store;
use std::{path::PathBuf, sync::Arc};
// use tmpdir::TmpDir;
use tokio::time::{sleep, Duration};
use tracing::info;
use veilid_core::{
    RouteId, RoutingContext, UpdateCallback, VeilidAPI, VeilidConfigInner, VeilidUpdate,
    VALID_CRYPTO_KINDS,
};

pub mod iroh;
pub mod tunnels;

#[tokio::test]
async fn test_tunnel() {
    //unsafe { backtrace_on_stack_overflow::enable() }

    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    /* four threads
    veilid cb -> updates channel
    tunnel1, wait for tunnel to come in and send an OK to output channel, spawn thread for listening to updates
    tunnel2, open tunnel, wait for write, send OK to output channel, spawn thread for waiting for updates
    test, listen for two Results and unwrap them
        */

    let (send_update, read_update) = broadcast::channel::<VeilidUpdate>(256);
    let (send_result, mut read_result) = mpsc::channel::<Result<()>>(2);

    let send_result1 = send_result.clone();
    let send_result2 = send_result.clone();

    let read_update1 = read_update;
    let read_update2 = send_update.subscribe();

    let (veilid, mut rx) = init_veilid(Some("tunnels_test_1".to_string()), &base_dir.join("peer1"))
        .await
        .expect("Unable to init veilid and store");

    let sender_handle = tokio::spawn(async move {
        while let Ok(update) = rx.recv().await {
            //println!("Received update: {:#?}", update);
            if let Err(err) = send_update.send(update) {
                eprintln!("Unable to process veilid update: {:?}", err);
            }
        }
    });

    println!("Cloning veilid API for tunnels");

    let v1 = veilid.clone();
    let v2 = veilid.clone();

    println!("Initializing route1");

    let router1 = v1.routing_context().unwrap();
    let (route_id1, route_id1_blob) = make_route(&v1).await.unwrap();

    println!("Initializing route2");
    let router2 = v2.routing_context().unwrap();
    let (route_id2, route_id2_blob) = make_route(&v2).await.unwrap();

    println!("Routes ready");

    let on_new_tunnel1: OnNewTunnelCallback = Arc::new(move |tunnel| {
        println!("New tunnel {:?}", tunnel);

        let send_result1 = send_result1.clone();

        tokio::spawn(async move {
            send_result1.send(Ok(())).await.unwrap();

            let (sender, mut reader) = tunnel;

            let result = reader.recv().await;

            if result.is_none() {
                send_result1
                    .send(Err(anyhow!("Unable to read first message")))
                    .await
                    .unwrap();
                return;
            }

            let raw = result.unwrap();

            let message = str::from_utf8(&raw.as_slice()).unwrap();

            if message.eq("Hello World!") {
                send_result1.send(Ok(())).await.unwrap();
            } else {
                send_result1
                    .send(Err(anyhow!("Got invalid message from tunnel {0}", message)))
                    .await
                    .unwrap();
            }

            let result = sender.send("Goodbye World!".as_bytes().to_vec()).await;

            send_result1
                .send(if let Ok(_) = result {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "Unable to send response tunnel {:?}",
                        result.unwrap_err()
                    ))
                })
                .await
                .unwrap();
        });
    });

    let tunnels1 = tunnels::TunnelManager::new(
        v1,
        router1,
        route_id1,
        route_id1_blob.clone(),
        Some(on_new_tunnel1),
    );
    let tunnels2 = tunnels::TunnelManager::new(v2, router2, route_id2, route_id2_blob, None);

    println!("Spawn tunnel1");
    let tunnel1_handle = tokio::spawn(async move {
        println!("Listening in tunnel1");
        tunnels1.listen(read_update1).await.unwrap();
    });

    println!("Spawn tunnel2");
    let tunnel2_handle = tokio::spawn(async move {
        let listening = tunnels2.clone();
        tokio::spawn(async move {
            println!("Listening in tunnel2");
            listening.listen(read_update2).await.unwrap();
        });

        sleep(Duration::from_secs(10)).await;

        let result = tunnels2.open(route_id1_blob).await;

        if result.is_err() {
            send_result2.send(Err(result.unwrap_err())).await.unwrap();
            return;
        }

        println!("Tunnel opened");
        send_result2.send(Ok(())).await.unwrap();

        let (sender, mut reader) = result.unwrap();

        // STart reading so the chanel isn't marked as closed
        let read_result = reader.recv();
        let result = sender.send("Hello World!".as_bytes().to_vec()).await;

        send_result2
            .send(if let Ok(_) = result {
                Ok(())
            } else {
                Err(anyhow!(
                    "Unable to send down tunnel {:?}",
                    result.unwrap_err()
                ))
            })
            .await
            .unwrap();

        let result = read_result.await;

        if result.is_none() {
            send_result2
                .send(Err(anyhow!("Unable to read first message")))
                .await
                .unwrap();
            return;
        }

        let raw = result.unwrap();

        let message = str::from_utf8(&raw.as_slice()).unwrap();

        if message.eq("Goodbye World!") {
            send_result2.send(Ok(())).await.unwrap();
        } else {
            send_result2
                .send(Err(anyhow!("Got invalid message from tunnel {0}", message)))
                .await
                .unwrap();
        }
    });

    let expected = 6;
    let mut count = 0;

    while count < expected {
        println!("wait result {0}", count + 1);
        count += 1;
        read_result.recv().await.unwrap().unwrap();
    }

    println!("cleanup");
    sender_handle.abort();
    tunnel1_handle.abort();
    tunnel2_handle.abort();
    veilid.shutdown().await;
}

async fn init_veilid(
    namespace: Option<String>,
    base_dir: &PathBuf,
) -> Result<(VeilidAPI, Receiver<VeilidUpdate>)> {
    let config_inner = config_for_dir(base_dir.to_path_buf(), namespace);

    let (tx, mut rx) = broadcast::channel(32);

    let update_callback: UpdateCallback = Arc::new(move |update| {
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(_) = tx.send(update) {
                // TODO:
                println!("receiver dropped");
                return;
            }
        });
    });

    println!("Init veilid");
    let veilid = veilid_core::api_startup_config(update_callback, config_inner).await?;

    println!("Attach veilid");

    veilid.attach().await?;

    println!("Wait for veilid network");

    while let Ok(update) = rx.recv().await {
        if let VeilidUpdate::Attachment(attachment_state) = update {
            if attachment_state.public_internet_ready && attachment_state.state.is_attached() {
                println!("Public internet ready!");
                break;
            }
        }
    }

    println!("Network ready");

    return Ok((veilid, rx));
}

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

        if result.is_ok() {
            return Ok(result.unwrap());
        }
    }
    return Err(anyhow!("Unable to create route, reached max retries"));
}

fn config_for_dir(base_dir: PathBuf, namespace: Option<String>) -> VeilidConfigInner {
    let namespace: String = namespace.unwrap_or("iroh-blobs".to_string());
    return VeilidConfigInner {
        program_name: "iroh-blobs".to_string(),
        namespace,
        protected_store: veilid_core::VeilidConfigProtectedStore {
            // avoid prompting for password, don't do this in production
            always_use_insecure_storage: true,
            directory: base_dir
                .join("protected_store")
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        },
        table_store: veilid_core::VeilidConfigTableStore {
            directory: base_dir.join("table_store").to_string_lossy().to_string(),
            ..Default::default()
        },
        block_store: veilid_core::VeilidConfigBlockStore {
            directory: base_dir.join("block_store").to_string_lossy().to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
}
