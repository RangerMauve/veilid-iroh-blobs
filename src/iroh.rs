use crate::init_veilid;
use crate::make_route;
use crate::tunnels::OnNewTunnelCallback;
use crate::tunnels::Tunnel;
use crate::tunnels::TunnelManager;

use anyhow::anyhow;
//use anyhow::Ok;
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
use iroh_blobs::format::collection::{Collection, SimpleStore};
use iroh_blobs::util::progress::IgnoreProgressSender;
use iroh_blobs::HashAndFormat;
use iroh_blobs::BlobFormat;
use iroh_blobs::Hash;
use iroh_blobs::Tag;
use std::pin::Pin;
use std::collections::HashMap;

use iroh_io::AsyncSliceReader;
use std::io::ErrorKind;
use std::io::Error;
use std::path::Path;
use std::process::Command;
use std::sync::Mutex;
use std::result;
use std::time::Duration;
use std::future::Future;
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

struct StoreWrapper<'a> {
    inner: &'a iroh_blobs::store::fs::Store,
}

impl<'a> SimpleStore for StoreWrapper<'a> {
    fn load(
        &self,
        hash: Hash,
    ) -> impl Future<Output = Result<Bytes, anyhow::Error>> + Send + '_ {
        Box::pin(async move {
            if let Some(blob_handle) = self.inner.get(&hash).await? {
                let mut reader = blob_handle.data_reader();
                let mut buffer = vec![0u8; reader.size().await? as usize];
                reader.read_at(0, buffer.len()).await?;
                Ok(Bytes::from(buffer))
            } else {
                Err(anyhow::anyhow!("Blob not found for hash: {:?}", hash))
            }
        })
    }
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

        let hash = tag.hash();
        return Ok(*hash);
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
    pub async fn create_collection(&self, collection_name: String) -> anyhow::Result<Hash> {
        // Create a new empty collection
        let collection = Collection::default();

        // Store the collection in the store and get its hash and format
        let temp_tag = collection.store(&self.store).await?;
        let collection_hash = *temp_tag.hash();
        let format = temp_tag.format();

        // Create a HashAndFormat
        let hash_and_format = HashAndFormat {
            hash: collection_hash,
            format,
        };

        // Create a Tag from the collection name
        let tag = Tag::from(collection_name.clone());

        // Set the tag in the store
        self.store.set_tag(tag, Some(hash_and_format)).await?;

        // Return the hash of the collection
        Ok(collection_hash)
    }

    pub async fn set_file(&self, collection_name: String, path: String, hash: Hash) -> Result<Hash> {
        // Load the collection by its hash from the store
        let collection_hash = self.collection_hash(collection_name.clone()).await?;

        let store_wrapper = StoreWrapper { inner: &self.store };

        let mut collection = Collection::load(collection_hash, &store_wrapper).await?;

        // Add the file to the collection
        collection.push(path, hash);

        // Store the updated collection and get the new root hash
        let temp_tag = collection.store(&self.store).await?;
        let new_collection_hash = *temp_tag.hash();
        let format = temp_tag.format();

        // Update the tag in the store
        let hash_and_format = HashAndFormat {
            hash: new_collection_hash,
            format,
        };
        let tag = Tag::from(collection_name.clone());
        self.store.set_tag(tag, Some(hash_and_format)).await?;

        // Return the new root hash of the updated collection
        Ok(new_collection_hash)
    }
    
    pub async fn get_file(&self, collection_name: String, path: String) -> Result<Hash> {
        // Load the collection by its hash from the store
        let collection_hash = self.collection_hash(collection_name.clone()).await?;

        let store_wrapper = StoreWrapper { inner: &self.store };

        let collection = Collection::load(collection_hash, &store_wrapper).await?;
    
        // Find the file by its path in the collection
        for (file_path, file_hash) in collection.iter() {
            if file_path == &path {
                return Ok(*file_hash);
            }
        }
    
        Err(anyhow!("File not found in the collection"))
    }

    pub async fn delete_file(&self, collection_name: String, path: String) -> Result<Hash> {
        // Load the collection by its hash from the store
        let collection_hash = self.collection_hash(collection_name.clone()).await?;

        let store_wrapper = StoreWrapper { inner: &self.store };

        let collection = Collection::load(collection_hash, &store_wrapper).await?;

        // Remove the file from the collection
        let new_entries = collection.iter()
            .filter(|(file_path, _)| file_path != &path)
            .cloned()
            .collect::<Vec<_>>();

        let mut new_collection = Collection::default();
        for (file_path, file_hash) in new_entries {
            new_collection.push(file_path, file_hash);
        }

        // Store the updated collection and get the new root hash
        let temp_tag = new_collection.store(&self.store).await?;
        let new_collection_hash = *temp_tag.hash();
        let format = temp_tag.format();

        // Update the tag in the store
        let hash_and_format = HashAndFormat {
            hash: new_collection_hash,
            format,
        };
        let tag = Tag::from(collection_name.clone());
        self.store.set_tag(tag, Some(hash_and_format)).await?;

        // Return the new root hash of the updated collection
        Ok(new_collection_hash)
    }
    
    pub async fn list_files(&self, collection_name: String) -> Result<Vec<String>> {
        // Load the collection by its hash from the store
        let collection_hash = self.collection_hash(collection_name.clone()).await?;

        let store_wrapper = StoreWrapper { inner: &self.store };

        let collection = Collection::load(collection_hash, &store_wrapper).await?;
    
        // Collect all file paths
        let file_paths: Vec<String> = collection.iter().map(|(file_path, _)| file_path.clone()).collect();
    
        Ok(file_paths)
    }

    pub async fn upload_to(&self, collection_name: String, path: String, file_stream: mpsc::Receiver<std::io::Result<Bytes>>) -> Result<Hash> {
        // Upload the file stream and get its hash
        let file_hash = self.upload_from_stream(file_stream).await?;
    
        // Add the uploaded file to the collection
        self.set_file(collection_name, path, file_hash).await
    }
    
    pub async fn collection_hash(&self, collection_name: String) -> Result<Hash> {
        // Create a Tag from the collection name
        let tag = Tag::from(collection_name.clone());

        // Retrieve the HashAndFormat associated with the tag
        if let Some(hash_and_format) = self.get_tag(&tag).await? {
            // Return the hash
            Ok(hash_and_format.hash)
        } else {
            Err(anyhow!("Collection {} not found", collection_name))
        }
    }

    async fn get_tag(&self, target_tag: &Tag) -> Result<Option<HashAndFormat>> {
        // Retrieve an iterator over all tags
        let mut tags_iter = self.store.tags().await?;
    
        // Iterate over the tags to find the matching tag
        while let Some(result) = tags_iter.next() {
            let (tag, hash_and_format) = result?;
            if &tag == target_tag {
                return Ok(Some(hash_and_format));
            }
        }
        Ok(None)
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

    assert!(has, "Blobs has hash after download");

    sender_handle.abort();
}
#[tokio::test]
async fn test_create_collection() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    // Initialize the blobs instance
    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None)
        .await
        .unwrap();

    // Call create_collection method
    let collection_name = "my_test_collection".to_string();
    let collection_hash = blobs.create_collection(collection_name.clone()).await.unwrap();

    // Ensure the collection hash is not empty
    assert!(!collection_hash.as_bytes().is_empty(), "Collection hash should not be empty");

    println!("Created collection with hash: {}", collection_hash);

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
    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None)
        .await
        .unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    let collection_hash = blobs.create_collection(collection_name.clone()).await.unwrap();
    assert!(!collection_hash.as_bytes().is_empty(), "Collection hash should not be empty");
    println!("Created collection with hash: {}", collection_hash);

    // Test set_file
    let file_path = "test_file.txt".to_string();

    
    // Create a temporary file to upload
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
        
    // Add debug statements
    println!("File hash: {}", file_hash);
    
    let has_file = blobs.has_hash(&file_hash).await;
    println!("Has file: {}", has_file);
    assert!(has_file, "Store should have the file hash after upload");
    
    // Before loading the collection in set_file
    println!("Collection hash in set_file: {}", collection_hash);
    
    
    let updated_collection_hash = blobs.set_file(collection_name.clone(), file_path.clone(), file_hash).await.unwrap();
    assert!(!updated_collection_hash.as_bytes().is_empty(), "Updated collection hash should not be empty");

    // Test get_file
    let retrieved_file_hash = blobs.get_file(collection_name.clone(), file_path.clone()).await.unwrap();
    assert_eq!(file_hash, retrieved_file_hash, "The file hash should match the uploaded file hash");

    // Test list_files
    let file_list = blobs.list_files(collection_name.clone()).await.unwrap();
    assert_eq!(file_list.len(), 1, "There should be one file in the collection");
    assert_eq!(file_list[0], file_path, "The file path should match the uploaded file path");

    // Test delete_file
    let new_collection_hash = blobs.delete_file(collection_name.clone(), file_path.clone()).await.unwrap();
    assert!(!new_collection_hash.as_bytes().is_empty(), "New collection hash after deletion should not be empty");

    let file_list_after_deletion = blobs.list_files(collection_name.clone()).await.unwrap();
    assert!(file_list_after_deletion.is_empty(), "There should be no files in the collection after deletion");

    // Test collection_hash
    let retrieved_collection_hash = blobs.collection_hash(collection_name.clone()).await.unwrap();
    assert_eq!(retrieved_collection_hash, new_collection_hash, "The retrieved collection hash should match the updated collection hash after deletion");

    // Test upload_to
    let new_file_path = "uploaded_file.txt".to_string();
    let (send_file, read_file) = mpsc::channel::<std::io::Result<Bytes>>(2);

    tokio::spawn(async move {
        let bytes = Bytes::from("test file content");
        if let Err(e) = send_file.send(std::io::Result::Ok(bytes)).await {
            // Convert the error into a `std::io::Error` and print it
            eprintln!(
                "Error sending file: {:?}",
                std::io::Error::new(std::io::ErrorKind::Other, e)
            );
        }
    });

    let new_file_collection_hash = blobs.upload_to(collection_name.clone(), new_file_path.clone(), read_file).await.unwrap();
    assert!(!new_file_collection_hash.as_bytes().is_empty(), "New collection hash after uploading a file should not be empty");

    // Clean up by shutting down blobs instance
    blobs.shutdown().await.unwrap();
}

