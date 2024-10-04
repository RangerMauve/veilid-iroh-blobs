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
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use veilid_core::{RouteId, RoutingContext, VeilidAPI, VeilidUpdate};
use serde::{Serialize, Deserialize};
use serde_cbor::{to_vec, from_slice};
use hex;

const NO: u8 = 0x00u8;
const YES: u8 = 0x01u8;
const HAS: u8 = 0x10u8;
const ASK: u8 = 0x11u8;
const DATA: u8 = 0x20u8;
const DONE: u8 = 0x22u8;
const ERR: u8 = 0xF0u8;

const DEFAULT_TIMEOUT: Duration = Duration::from_millis(4000);

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Collection {
    files: HashMap<String, Hash>,
}

#[derive(Clone)]
pub struct VeilidIrohBlobs {
    tunnels: TunnelManager,
    store: iroh_blobs::store::fs::Store,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    collection_hashes: Arc<RwLock<HashMap<String, Hash>>>,
    base_dir: PathBuf,
}

impl VeilidIrohBlobs {
    pub async fn from_directory(base_dir: &PathBuf, namespace: Option<String>) -> Result<Self> {
        let (veilid, updates, store) = init_deps(namespace, base_dir).await?;

        let router = veilid.routing_context().unwrap();
        let (route_id, route_id_blob) = make_route(&veilid).await.unwrap();

        let blobs = Self::new(
            veilid,
            router,
            route_id_blob,
            route_id,
            updates,
            store,
            base_dir.clone(), // Pass base_dir
        );
    
        // Load the collection_hashes HashMap
        let hash_file_path = base_dir.join("collection_hashes_hash");
        if hash_file_path.exists() {
            let hash_str = std::fs::read_to_string(hash_file_path)?;
            let hash_bytes = hex::decode(hash_str.trim())?;
            if hash_bytes.len() != 32 {
                return Err(anyhow!("Invalid hash length: expected 32 bytes, got {}", hash_bytes.len()));
            }
            let hash_array: [u8; 32] = hash_bytes.as_slice().try_into().unwrap();
            let hash = Hash::from_bytes(hash_array);

            // Retrieve the serialized HashMap from the store
            let serialized_map = blobs.read_bytes(hash).await?;
            let map: HashMap<String, Hash> = from_slice(&serialized_map)?;

            // Set the collection_hashes HashMap
            {
                let mut collection_hashes = blobs.collection_hashes.write().await;
                *collection_hashes = map;
            }
        }
    
        Ok(blobs)
    }

    pub fn new(
        veilid: VeilidAPI,
        router: RoutingContext,
        route_id_blob: Vec<u8>,
        route_id: RouteId,
        updates: Receiver<VeilidUpdate>,
        store: iroh_blobs::store::fs::Store,
        base_dir: PathBuf, 
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
            collection_hashes: Arc::new(RwLock::new(HashMap::new())),
            base_dir,
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
        // Serialize the collection_hashes HashMap
        let map = self.collection_hashes.read().await;
        let serialized_map = serde_cbor::to_vec(&*map)?;

        // Log: Writing serialized collection_hashes to a temporary file
        println!("Writing serialized collection_hashes to a temporary file...");

        // Write the serialized data to a temporary file
        let temp_path = self.base_dir.join("collection_hashes.cbor");
        std::fs::write(&temp_path, &serialized_map)?;

        // Convert the path to an absolute path
        let absolute_temp_path = std::fs::canonicalize(&temp_path)?;

        // Log: Uploading the serialized data from file to the store
        println!("Uploading the serialized data from file to the store...");

        // Upload the serialized data to the store using upload_from_path
        let collection_hashes_hash = self.upload_from_path(absolute_temp_path).await?;

        // Write the hash to a file in the base directory
        let hash_file_path = self.base_dir.join("collection_hashes_hash");
        let hash_hex = hex::encode(collection_hashes_hash.as_bytes());
        std::fs::write(hash_file_path, hash_hex)?;

        // Shutdown the handles
        let handles = self.handles.lock().unwrap();
        for handle in handles.iter() {
            handle.abort();
        }
        self.tunnels.shutdown().await?;
        self.store.shutdown().await;
        Ok(())
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
        // Log: Starting upload from stream
        println!("Starting upload from stream...");

        let stream = ReceiverStream::new(receiver);
        let progress = IgnoreProgressSender::<ImportProgress>::default();
        
        // Log: Importing stream to store
        println!("Importing stream to store...");
        
        let (tag, _) = self
            .store
            .import_stream(stream, BlobFormat::Raw, progress)
            .await?;

                
        // Log: Stream imported successfully
        println!("Stream imported successfully, generating hash...");

        let hash = tag.hash().clone();

        // Log: Upload completed with hash
        println!("Upload completed successfully with hash: {:?}", hash);

        return Ok(hash);
    }
    async fn read_bytes(&self, hash: Hash) -> Result<Vec<u8>> {
        let mut receiver = self.read_file(hash).await?;
        let mut data = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            data.extend_from_slice(&chunk?);
        }
        Ok(data)
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
        
        println!("Creating a new empty collection with name: {}", collection_name);
        
        // Create a new empty collection
        let collection = Collection::default();
    
         // Log: Serializing the collection to CBOR
        println!("Serializing the collection to CBOR...");
    
        // Serialize the collection to CBOR
        let cbor_data = to_vec(&collection)?;

        // Log: Writing CBOR data to a temporary file
        println!("Writing CBOR data to a temporary file...");
        
        // Create a temporary file to store the CBOR data
        let temp_path = self.base_dir.join(format!("{}_collection.cbor", collection_name));
        std::fs::write(&temp_path, &cbor_data)?;
        
        // Convert the path to an absolute path
        let absolute_temp_path = std::fs::canonicalize(&temp_path)?;

        // Log: Uploading the CBOR data from file to the store
        println!("Uploading the CBOR data from file to the store...");

        // Upload the CBOR data to the store using upload_from_path
        let collection_hash = self.upload_from_path(absolute_temp_path).await?;

        // Log: Storing the collection hash in the HashMap
        println!("Storing the collection hash in the HashMap...");

        // Store the collection hash in the HashMap
        {
            let mut map = self.collection_hashes.write().await;
            map.insert(collection_name, collection_hash);
        }

        // Log: Collection created successfully
        println!("Collection created successfully with hash: {:?}", collection_hash);

        Ok(collection_hash)
    }
    

    pub async fn set_file(&self, collection_name: String, path: String, hash: Hash) -> Result<Hash> {
        // Retrieve the collection's current hash
        let collection_hash = self.collection_hash(&collection_name).await?;
    
        // Read the collection data from the store
        let collection_data = self.read_bytes(collection_hash).await?;
        let mut collection: Collection = from_slice(&collection_data)?;
    
        // Add or update the file in the collection
        collection.files.insert(path, hash);
    
        // Serialize the updated collection to CBOR
        let cbor_data = to_vec(&collection)?;
        
            
        // Write the CBOR data to a temporary file
        let temp_path = self.base_dir.join(format!("{}_updated.cbor", collection_name));
        std::fs::write(&temp_path, &cbor_data)?;

        // Convert the path to an absolute path
        let absolute_temp_path = std::fs::canonicalize(&temp_path)?;

        // Upload the updated collection
        let new_collection_hash = self.upload_from_path(absolute_temp_path).await?;
        
        // Update the collection hash in the HashMap
        {
            let mut map = self.collection_hashes.write().await;
            map.insert(collection_name, new_collection_hash);
        }
    
        Ok(new_collection_hash)
    }
    
    
    pub async fn get_file(&self, collection_name: String, path: String) -> Result<Hash> {
        // Retrieve the collection's current hash
        let collection_hash = self.collection_hash(&collection_name).await?;
    
        // Read the collection data from the store
        let collection_data = self.read_bytes(collection_hash).await?;
        let collection: Collection = from_slice(&collection_data)?;
    
        // Get the file hash
        if let Some(file_hash) = collection.files.get(&path) {
            Ok(*file_hash)
        } else {
            Err(anyhow!("File not found in the collection"))
        }
    }
    pub async fn delete_file(&self, collection_name: String, path: String) -> Result<Hash> {
        // Retrieve the collection's current hash
        let collection_hash = self.collection_hash(&collection_name).await?;
    
        // Read and deserialize the collection
        let collection_data = self.read_bytes(collection_hash).await?;
        let mut collection: Collection = from_slice(&collection_data)?;
    
        // Remove the file from the collection
        collection.files.remove(&path);
    
        // Serialize the updated collection to CBOR
        let cbor_data = to_vec(&collection)?;
    
        // Write the CBOR data to a temporary file
        let temp_path = self.base_dir.join(format!("{}_deleted.cbor", collection_name));
        std::fs::write(&temp_path, &cbor_data)?;

        // Convert the path to an absolute path
        let absolute_temp_path = std::fs::canonicalize(&temp_path)?;

        // Upload the updated collection
        let new_collection_hash = self.upload_from_path(absolute_temp_path).await?;

        // Update the collection hash in the HashMap
        {
            let mut map = self.collection_hashes.write().await;
            map.insert(collection_name, new_collection_hash);
        }
    
        Ok(new_collection_hash)
    }
    pub async fn list_files(&self, collection_name: String) -> Result<Vec<String>> {
        // Retrieve the collection's current hash
        let collection_hash = self.collection_hash(&collection_name).await?;
    
        // Read and deserialize the collection
        let collection_data = self.read_bytes(collection_hash).await?;
        let collection: Collection = from_slice(&collection_data)?;
    
        // Return the list of file paths
        Ok(collection.files.keys().cloned().collect())
    }
    
    pub async fn upload_to(&self, collection_name: String, path: String, file_stream: mpsc::Receiver<std::io::Result<Bytes>>) -> Result<Hash> {
        // Upload the file stream and get its hash
        let file_hash = self.upload_from_stream(file_stream).await?;

        // Add the uploaded file to the collection
        self.set_file(collection_name, path, file_hash).await
    }
    pub async fn collection_hash(&self, collection_name: &str) -> Result<Hash> {
        let map = self.collection_hashes.read().await;
        if let Some(hash) = map.get(collection_name) {
            Ok(*hash)
        } else {
            Err(anyhow!("Collection {} not found", collection_name))
        }
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
    let store1 = iroh_blobs::store::fs::Store::load(store1_dir.clone())
        .await
        .unwrap();

    let mut store2_dir = base_dir.clone();
    store2_dir.push("peer2");
    let store2 = iroh_blobs::store::fs::Store::load(store2_dir.clone())
        .await
        .unwrap();
    let router1 = v1.routing_context().unwrap();
    let (route_id1, route_id1_blob) = make_route(&v1).await.unwrap();

    println!("Initializing route2");
    let router2 = v2.routing_context().unwrap();
    let (route_id2, route_id2_blob) = make_route(&v2).await.unwrap();

    let blobs1 = VeilidIrohBlobs::new(v1, router1, route_id1_blob, route_id1, read_update1, store1, store1_dir.clone(),);
    let blobs2 = VeilidIrohBlobs::new(v2, router2, route_id2_blob, route_id2, read_update2, store2, store2_dir.clone(),);

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

    // Log: Initializing blobs instance
    println!("Initializing blobs instance from directory: {:?}", base_dir);


    // Initialize the blobs instance
    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None)
        .await
        .unwrap();

    // Log: Creating collection
    println!("Creating collection...");

    // Call create_collection method
    let collection_name = "my_test_collection".to_string();
    let collection_hash = blobs.create_collection(collection_name.clone()).await.unwrap();

    // Ensure the collection hash is not empty
    assert!(!collection_hash.as_bytes().is_empty(), "Collection hash should not be empty");

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
    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None)
        .await
        .unwrap();

    // Log: Creating collection
    println!("Creating collection...");

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

    // Ensure the path is absolute
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();

    // Add debug statements
    println!("File hash: {}", file_hash);

    let has_file = blobs.has_hash(&file_hash).await;
    println!("Has file: {}", has_file);
    assert!(has_file, "Store should have the file hash after upload");

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
    let retrieved_collection_hash = blobs.collection_hash(&collection_name).await.unwrap();
    assert_eq!(retrieved_collection_hash, new_collection_hash, "The retrieved collection hash should match the updated collection hash after deletion");

      // Test upload_to (now using `set_file` with `upload_from_path`)
      let new_file_path = "uploaded_file.txt".to_string();
      let temp_new_file_path = base_dir.join(&new_file_path);
      std::fs::write(&temp_new_file_path, "test file content for upload_to").unwrap();
      let absolute_new_file_path = std::fs::canonicalize(&temp_new_file_path).unwrap();
  
      // Upload the new file and add it to the collection
      let new_file_hash = blobs.upload_from_path(absolute_new_file_path).await.unwrap();
      let new_file_collection_hash = blobs.set_file(collection_name.clone(), new_file_path.clone(), new_file_hash).await.unwrap();
      assert!(!new_file_collection_hash.as_bytes().is_empty(), "New collection hash after uploading a file should not be empty");
  
    // Clean up by shutting down blobs instance
    blobs.shutdown().await.unwrap();
}

