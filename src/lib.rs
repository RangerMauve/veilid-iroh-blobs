use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use core::str;
use futures_lite::{Stream, StreamExt};
use iroh::VeilidIrohBlobs;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tunnels::OnNewRouteCallback;
use tunnels::OnNewTunnelCallback;
use tunnels::OnRouteDisconnectedCallback;
use veilid_core::{
    RouteId, UpdateCallback, VeilidAPI, VeilidConfigInner, VeilidUpdate, VALID_CRYPTO_KINDS,
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

            let message = str::from_utf8(raw.as_slice()).unwrap();

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
                .send(if result.is_ok() {
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
        None,
        None,
    );
    let tunnels2 =
        tunnels::TunnelManager::new(v2, router2, route_id2, route_id2_blob, None, None, None);

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
            .send(if result.is_ok() {
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

        let message = str::from_utf8(raw.as_slice()).unwrap();

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

#[tokio::test]
async fn test_tunnel_route_reset() {
    //unsafe { backtrace_on_stack_overflow::enable() }

    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let (send_update, read_update) = broadcast::channel::<VeilidUpdate>(256);
    let (send_result, mut read_result) = mpsc::channel::<Result<()>>(2);

    let send_result1 = send_result.clone();
    let send_result2 = send_result.clone();

    let (veilid, mut rx) = init_veilid(Some("tunnels_test".to_string()), &base_dir)
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

    let on_new_route: OnNewRouteCallback = Arc::new(move |_, _| {
        let send_result = send_result1.clone();
        tokio::spawn(async move {
            send_result.send(Ok(())).await.unwrap();
        });
    });
    let on_disconnected: OnRouteDisconnectedCallback = Arc::new(move || {
        let send_result = send_result2.clone();
        tokio::spawn(async move {
            send_result.send(Ok(())).await.unwrap();
        });
    });

    let router = veilid.routing_context().unwrap();
    let (route_id, route_id_blob) = make_route(&veilid).await.unwrap();

    let tunnels = tunnels::TunnelManager::new(
        veilid.clone(),
        router,
        route_id,
        route_id_blob,
        None,
        Some(on_disconnected),
        Some(on_new_route),
    );

    tokio::spawn(async move {
        tunnels.listen(read_update).await.unwrap();
    });

    veilid.release_private_route(route_id).unwrap();

    let expected = 2;
    let mut count = 0;

    while count < expected {
        println!("wait result {0}", count + 1);
        count += 1;
        read_result.recv().await.unwrap().unwrap();
    }

    println!("cleanup");
    sender_handle.abort();
    veilid.shutdown().await;
}

#[tokio::test]
async fn test_blobs() {
    //unsafe { backtrace_on_stack_overflow::enable() }

    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    let hash = blobs
        .upload_from_path(std::fs::canonicalize(Path::new("./README.md")).unwrap())
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

    let blobs1 = VeilidIrohBlobs::new(
        v1,
        router1,
        route_id1_blob,
        route_id1,
        read_update1,
        store1,
        None,
        None,
    );
    let blobs2 = VeilidIrohBlobs::new(
        v2,
        router2,
        route_id2_blob,
        route_id2,
        read_update2,
        store2,
        None,
        None,
    );

    let hash = blobs1
        .upload_from_path(std::fs::canonicalize(Path::new("./README.md")).unwrap())
        .await
        .unwrap();

    blobs2
        .download_file_from(blobs1.route_id_blob().await, &hash)
        .await
        .unwrap();

    let has = blobs2.has_hash(&hash).await;

    assert!(has, "Blobs has hash after download");

    sender_handle.abort();
}

#[tokio::test]
async fn test_create_collection() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    // Log: Initializing blobs instance
    println!("Initializing blobs instance from directory: {:?}", base_dir);

    // Initialize the blobs instance
    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Log: Creating collection
    println!("Creating collection...");

    // Call create_collection method
    let collection_name = "my_test_collection".to_string();
    let collection_hash = blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    // Ensure the collection hash is not empty
    assert!(
        !collection_hash.as_bytes().is_empty(),
        "Collection hash should not be empty"
    );

    println!("The collection was created with hash: {}", collection_hash);

    // Verify that the collection exists in the store
    let has_collection = blobs.has_hash(&collection_hash).await;
    assert!(
        has_collection,
        "Blobs should have the collection hash after creation"
    );

    // Clean up by shutting down blobs instance
    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_collection_operations() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    // Initialize the blobs instance
    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Log: Creating collection
    println!("Creating collection...");

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    let collection_hash = blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();
    assert!(
        !collection_hash.as_bytes().is_empty(),
        "Collection hash should not be empty"
    );
    println!("Created collection with hash: {}", collection_hash);

    // Test set_file
    let file_path = "test_file.txt".to_string();

    // Create a temporary file to upload
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();

    // Ensure the path is absolute
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();

    // Add debug statements
    println!("File hash: {}", file_hash);

    let has_file = blobs.has_hash(&file_hash).await;
    println!("Has file: {}", has_file);
    assert!(has_file, "Store should have the file hash after upload");

    let updated_collection_hash = blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &file_hash)
        .await
        .unwrap();
    assert!(
        !updated_collection_hash.as_bytes().is_empty(),
        "Updated collection hash should not be empty"
    );

    // Test get_file
    let retrieved_file_hash = blobs
        .get_file(&collection_name.clone(), &file_path.clone())
        .await
        .unwrap();
    assert_eq!(
        file_hash, retrieved_file_hash,
        "The file hash should match the uploaded file hash"
    );

    // Test list_files
    let file_list = blobs.list_files(&collection_name.clone()).await.unwrap();
    assert_eq!(
        file_list.len(),
        1,
        "There should be one file in the collection"
    );
    assert_eq!(
        file_list[0], file_path,
        "The file path should match the uploaded file path"
    );

    // Test delete_file
    let new_collection_hash = blobs
        .delete_file(&collection_name.clone(), &file_path.clone())
        .await
        .unwrap();
    assert!(
        !new_collection_hash.as_bytes().is_empty(),
        "New collection hash after deletion should not be empty"
    );

    let file_list_after_deletion = blobs.list_files(&collection_name.clone()).await.unwrap();
    assert!(
        file_list_after_deletion.is_empty(),
        "There should be no files in the collection after deletion"
    );

    // Test collection_hash
    let retrieved_collection_hash = blobs.collection_hash(&collection_name).await.unwrap();
    assert_eq!(
        retrieved_collection_hash, new_collection_hash,
        "The retrieved collection hash should match the updated collection hash after deletion"
    );

    // Test upload_to (now using `set_file` with `upload_from_path`)
    let new_file_path = "uploaded_file.txt".to_string();
    let temp_new_file_path = base_dir.join(&new_file_path);
    std::fs::write(&temp_new_file_path, "test file content for upload_to").unwrap();
    let absolute_new_file_path = std::fs::canonicalize(&temp_new_file_path).unwrap();

    // Upload the new file and add it to the collection
    let new_file_hash = blobs
        .upload_from_path(absolute_new_file_path)
        .await
        .unwrap();
    let new_file_collection_hash = blobs
        .set_file(
            &collection_name.clone(),
            &new_file_path.clone(),
            &new_file_hash,
        )
        .await
        .unwrap();
    assert!(
        !new_file_collection_hash.as_bytes().is_empty(),
        "New collection hash after uploading a file should not be empty"
    );

    // Clean up by shutting down blobs instance
    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_set_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    let collection_hash = blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    assert!(
        !collection_hash.as_bytes().is_empty(),
        "Collection hash should not be empty"
    );
    println!("Created collection with hash: {}", collection_hash);

    // Attempt to retrieve the tag
    println!(
        "Attempting to retrieve tag for collection: {}",
        collection_name
    );
    match blobs.get_tag(&collection_name).await {
        Ok(tag_hash) => {
            println!("Successfully retrieved tag: {:?}", tag_hash);
        }
        Err(e) => {
            println!("Error retrieving tag: {:?}", e);
        }
    }

    // Test set_file
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    println!("File hash: {}", file_hash);

    let has_file = blobs.has_hash(&file_hash).await;
    println!("Has file: {}", has_file);
    assert!(has_file, "Store should have the file hash after upload");

    let updated_collection_hash = blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &file_hash)
        .await
        .unwrap();
    assert!(
        !updated_collection_hash.as_bytes().is_empty(),
        "Updated collection hash should not be empty"
    );

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_get_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    // Test set_file
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &file_hash)
        .await
        .unwrap();

    // Test get_file
    let retrieved_file_hash = blobs
        .get_file(&collection_name.clone(), &file_path.clone())
        .await
        .unwrap();
    assert_eq!(
        file_hash, retrieved_file_hash,
        "The file hash should match the uploaded file hash"
    );

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_delete_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    // Test set_file
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &file_hash)
        .await
        .unwrap();

    // Test delete_file
    let new_collection_hash = blobs
        .delete_file(&collection_name.clone(), &file_path.clone())
        .await
        .unwrap();
    assert!(
        !new_collection_hash.as_bytes().is_empty(),
        "New collection hash after deletion should not be empty"
    );

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_list_files() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    // Test set_file
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &file_hash)
        .await
        .unwrap();

    // Test list_files
    let file_list = blobs.list_files(&collection_name.clone()).await.unwrap();
    assert_eq!(
        file_list.len(),
        1,
        "There should be one file in the collection"
    );
    assert_eq!(
        file_list[0], file_path,
        "The file path should match the uploaded file path"
    );

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_collection_hash() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    let initial_collection_hash = blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    // Test collection_hash
    let retrieved_collection_hash = blobs.collection_hash(&collection_name).await.unwrap();
    assert_eq!(
        initial_collection_hash, retrieved_collection_hash,
        "The retrieved collection hash should match the created collection hash"
    );

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_upload_to() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    // Create a temporary file to upload
    let file_path = "uploaded_file.txt".to_string();
    let temp_file_path = base_dir.join("uploaded_file.txt");
    std::fs::write(&temp_file_path, "test file content for upload_to").unwrap();
    let absolute_temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    // Create a file stream using mpsc
    let (sender, receiver) = mpsc::channel(1);
    let file_content = Bytes::from("this is a test file content");

    // Spawn a task to send the file content via the sender
    tokio::spawn(async move {
        sender.send(Ok(file_content)).await.unwrap();
    });

    // Call upload_to with the file stream and path
    let file_path = "test_file.txt".to_string();
    let file_hash = blobs
        .upload_to(&collection_name, &file_path, receiver)
        .await
        .unwrap();

    // Add the uploaded file to the collection
    let new_file_collection_hash = blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &file_hash)
        .await
        .unwrap();
    assert!(
        !new_file_collection_hash.as_bytes().is_empty(),
        "New collection hash after uploading a file should not be empty"
    );

    blobs.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_missing_collection() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();

    // Attempt to retrieve a file from a non-existent collection
    let result = blobs
        .get_file(
            &"non_existent_collection".to_string(),
            &"some_file.txt".to_string(),
        )
        .await;
    assert!(
        result.is_err(),
        "Retrieving file from non-existent collection should fail"
    );

    // Attempt to list files in a non-existent collection
    let result = blobs
        .list_files(&"non_existent_collection".to_string())
        .await;
    assert!(
        result.is_err(),
        "Listing files from non-existent collection should fail"
    );
}

#[tokio::test]
async fn test_overwrite_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None, None, None)
        .await
        .unwrap();
    let collection_name = "my_test_collection".to_string();
    blobs
        .create_collection(&collection_name.clone())
        .await
        .unwrap();

    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs
        .upload_from_path(temp_file_path.clone())
        .await
        .unwrap();
    blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &file_hash)
        .await
        .unwrap();

    // Overwrite the file with new content
    std::fs::write(&temp_file_path, "new test file content").unwrap();
    let new_file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    let updated_collection_hash = blobs
        .set_file(&collection_name.clone(), &file_path.clone(), &new_file_hash)
        .await
        .unwrap();

    assert!(
        !updated_collection_hash.as_bytes().is_empty(),
        "Updated collection hash should not be empty"
    );

    // Ensure that the file hash was updated
    let retrieved_file_hash = blobs
        .get_file(&collection_name.clone(), &file_path.clone())
        .await
        .unwrap();
    assert_eq!(
        new_file_hash, retrieved_file_hash,
        "The file hash should be updated after overwrite"
    );
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
            if tx.send(update).is_err() {
                // TODO:
                println!("receiver dropped");
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

    Ok((veilid, rx))
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

    Ok((veilid, rx, store))
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
