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
use iroh_io::AsyncSliceReaderExt;
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
use std::borrow::Borrow;
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

#[derive(Clone)]
pub struct VeilidIrohBlobs {
    tunnels: TunnelManager,
    store: iroh_blobs::store::fs::Store,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
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
            base_dir.clone(), 
        );
    
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
    pub async fn create_collection(&self, collection_name: &String) -> anyhow::Result<Hash> {
        println!("Creating a new empty collection with name: {}", collection_name);
    
        // Create a new empty HashMap for the collection
        let collection: HashMap<String, Hash> = HashMap::new();
    
        // Serialize the HashMap to CBOR
        let cbor_data = to_vec(&collection)?;
    
        // Write the CBOR data to a temporary file
        let temp_path = self.base_dir.join(format!("{}_collection.cbor", collection_name));
        std::fs::write(&temp_path, &cbor_data)?;
    
        // Convert the path to an absolute path and upload it to the store
        let absolute_temp_path = std::fs::canonicalize(&temp_path)?;
        let collection_hash = self.upload_from_path(absolute_temp_path).await?;
    
        // Store the new collection hash using the tag system
        self.store.set_tag(
            collection_name.to_string().into(),
            Some(HashAndFormat::new(collection_hash, BlobFormat::Raw)),
        ).await?;
    
        println!("Collection created successfully with hash: {:?}", collection_hash);
        Ok(collection_hash)
    }
    
    
    
    pub async fn set_file(&self, collection_name: &String, path: &String, file_hash: &Hash) -> Result<Hash> {
         // Retrieve the current collection hash from the tag
        println!("Attempting to retrieve tag for collection: {}", collection_name);

        let collection_hash = self.collection_hash(&collection_name).await?;

        // Fetch collection data by its hash
        let entry = self
            .store
            .get(&collection_hash)
            .await?
            .ok_or_else(|| anyhow!("Collection not found for hash: {}", collection_hash))?;

        let mut reader = entry.data_reader();
        let collection_data: Bytes = reader.read_to_end().await?;

        let mut collection: HashMap<String, Hash> = from_slice(&collection_data).map_err(|err| {
            println!("Error deserializing collection: {:?}", err);
            anyhow!("Failed to deserialize collection: {:?}", err)
        })?;

        // Add or update the file in the collection (HashMap)
        collection.insert(path.clone(), file_hash.clone());

        // Serialize the updated HashMap to CBOR
        let cbor_data = to_vec(&collection)?;

        // Upload the updated collection data to the store
        let temp_path = self.base_dir.join("updated_collection.cbor");
        std::fs::write(&temp_path, &cbor_data)?;
        let absolute_temp_path = std::fs::canonicalize(&temp_path)?;
        let new_collection_hash = self.upload_from_path(absolute_temp_path).await?;

        // Log: Print the new collection hash
        println!("New collection hash: {:?}", new_collection_hash);

        // Store the new collection hash with the tag
        self.store_tag(&collection_name, &new_collection_hash).await?;
        Ok(new_collection_hash)
    }
    pub async fn get_file(&self, collection_name: &String, path: &String) -> Result<Hash> {
        // Retrieve the current collection hash from the tag
        let collection_hash = self.get_tag(&collection_name).await?;
    
        // Use the store to retrieve the entry for the collection hash
        let collection_entry = self.store.get(&collection_hash).await?;
    
        // Check if the collection exists
        if let Some(entry) = collection_entry {
            let mut reader = entry.data_reader();
    
            // Read the data (which should be the serialized HashMap)
            let collection_data: Bytes = reader.read_to_end().await?;
            let collection: HashMap<String, Hash> = from_slice(&collection_data)?;
    
            // Return the file hash for the given path
            collection.get(path).cloned().ok_or_else(|| anyhow!("File not found"))
        } else {
            Err(anyhow!("Collection not found"))
        }
    }
    
    

    
    pub async fn delete_file(&self, collection_name: &String, path: &String) -> Result<Hash> {
        // Retrieve the current collection hash from the tag
        let collection_tag = self.get_tag(&collection_name).await?;
    
        // Use the store to get the collection data by its tag
        let collection_entry = self.store.get(&collection_tag).await?;
        
        // If the collection exists, read and deserialize it
        let mut collection: HashMap<String, Hash> = if let Some(entry) = collection_entry {
            let mut reader = entry.data_reader();
            let collection_data: Bytes = reader.read_to_end().await?;
            from_slice(&collection_data)?
        } else {
            HashMap::new()
        };
    
        // Remove the file from the collection
        collection.remove(path);
    
        // Serialize the updated HashMap to CBOR
        let cbor_data = to_vec(&collection)?;
    
        // Write the CBOR data to a temporary file and upload it to the store
        let temp_path = self.base_dir.join("updated_collection.cbor");
        std::fs::write(&temp_path, &cbor_data)?;
        let absolute_temp_path = std::fs::canonicalize(&temp_path)?;
        let new_collection_hash = self.upload_from_path(absolute_temp_path).await?;
    
        // Store the new collection hash with the tag
        self.store_tag(&collection_name, &new_collection_hash).await?;
    
        Ok(new_collection_hash)
    }
    
    
    pub async fn list_files(&self, collection_name: &String) -> Result<Vec<String>> {
        // Retrieve the current collection hash from the tag
        let collection_hash = self.get_tag(&collection_name).await?;
    
        // Use the store to retrieve the entry for the collection hash
        let collection_entry = self.store.get(&collection_hash).await?;
    
        // Check if the collection exists
        if let Some(entry) = collection_entry {
            let mut reader = entry.data_reader();

        // Read the data (which should be the serialized HashMap)
        let collection_data: Bytes = reader.read_to_end().await?;
        let collection: HashMap<String, Hash> = from_slice(&collection_data)?;

        // Return the list of file paths (the keys in the HashMap)
        Ok(collection.keys().cloned().collect())
        
        } else {
            Err(anyhow!("Collection not found"))
        }
    }
    
    
    pub async fn upload_to(&self, collection_name: &String, path: &String, file_stream: mpsc::Receiver<std::io::Result<Bytes>>) -> Result<Hash> {
        // Upload the file stream and get its hash
        let file_hash = self.upload_from_stream(file_stream).await?;

        // Add the uploaded file to the collection
        self.set_file(&collection_name, &path, &file_hash).await
    }
    pub async fn collection_hash(&self, collection_name: &str) -> Result<Hash> {
        // Retrieve the tag from the store instead of using the in-memory cache
        match self.get_tag(collection_name).await {
            Ok(hash) => Ok(hash),
            Err(e) => {
                println!("Error retrieving tag for collection: {:?}", e);
                Err(anyhow!("Collection {} not found", collection_name))
            }
        }
    }
    pub async fn store_tag(&self, collection_name: &str, collection_hash: &Hash) -> Result<()> {
        println!("Storing tag for collection: {} with hash: {:?}", collection_name, collection_hash);
        
        // Store the tag
        self.store.set_tag(
            collection_name.to_string().into(), 
            Some(HashAndFormat::new(*collection_hash, BlobFormat::Raw))
        ).await?;
    
        // Introducing a short delay to ensure that tag is persisted
        tokio::time::sleep(Duration::from_millis(100)).await;
    
        // Verify that the tag was actually stored
        match self.get_tag(collection_name).await {
            Ok(stored_hash) => {
                if stored_hash == *collection_hash {
                    println!("Tag successfully stored for collection: {}", collection_name);
                } else {
                    return Err(anyhow!("Mismatch in stored tag for collection: {}", collection_name));
                }
            },
            Err(e) => {
                return Err(anyhow!("Failed to retrieve tag after storing for collection: {}. Error: {:?}", collection_name, e));
            }
        }
        
        Ok(())
    }
    
    pub async fn get_tag(&self, collection_name: &str) -> Result<Hash> {
        println!("Retrieving tag for collection: {}", collection_name);
        let mut tags = self.store.tags().await?;
    
        let collection_name_bytes = collection_name.as_bytes();
    
        while let Some(tag_result) = tags.next() {
            let (tag, hash_and_format) = tag_result.map_err(|e| {
                println!("Error reading tags: {:?}", e);
                anyhow!("Error reading tags: {:?}", e)
            })?;
    
            // Logging for better insight
            println!("Checking tag: {:?}", tag);
    
            // Directly compare tag bytes with collection_name bytes
            if tag.0.as_ref().eq(collection_name_bytes) {
                println!("Found matching tag for collection: {}", collection_name);
                return Ok(hash_and_format.hash);
            } else {
                println!("Tag {:?} did not match {:?}", &tag, collection_name_bytes);
            }
        }
    
        Err(anyhow!("Tag not found for collection: {}", collection_name))
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
    let collection_hash = blobs.create_collection(&collection_name.clone()).await.unwrap();

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
    let collection_hash = blobs.create_collection(&collection_name.clone()).await.unwrap();
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

    let updated_collection_hash = blobs.set_file(&collection_name.clone(), &file_path.clone(), &file_hash).await.unwrap();
    assert!(!updated_collection_hash.as_bytes().is_empty(), "Updated collection hash should not be empty");

    // Test get_file
    let retrieved_file_hash = blobs.get_file(&collection_name.clone(), &file_path.clone()).await.unwrap();
    assert_eq!(file_hash, retrieved_file_hash, "The file hash should match the uploaded file hash");

    // Test list_files
    let file_list = blobs.list_files(&collection_name.clone()).await.unwrap();
    assert_eq!(file_list.len(), 1, "There should be one file in the collection");
    assert_eq!(file_list[0], file_path, "The file path should match the uploaded file path");

    // Test delete_file
    let new_collection_hash = blobs.delete_file(&collection_name.clone(), &file_path.clone()).await.unwrap();
    assert!(!new_collection_hash.as_bytes().is_empty(), "New collection hash after deletion should not be empty");

    let file_list_after_deletion = blobs.list_files(&collection_name.clone()).await.unwrap();
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
      let new_file_collection_hash = blobs.set_file(&collection_name.clone(), &new_file_path.clone(), &new_file_hash).await.unwrap();
      assert!(!new_file_collection_hash.as_bytes().is_empty(), "New collection hash after uploading a file should not be empty");
  
    // Clean up by shutting down blobs instance
    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_set_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    let collection_hash = blobs.create_collection(&collection_name.clone()).await.unwrap();

    assert!(!collection_hash.as_bytes().is_empty(), "Collection hash should not be empty");
    println!("Created collection with hash: {}", collection_hash);
    
    // Attempt to retrieve the tag
    println!("Attempting to retrieve tag for collection: {}", collection_name);
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

    let updated_collection_hash = blobs.set_file(&collection_name.clone(), &file_path.clone(), &file_hash).await.unwrap();
    assert!(!updated_collection_hash.as_bytes().is_empty(), "Updated collection hash should not be empty");

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_get_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs.create_collection(&collection_name.clone()).await.unwrap();

    // Test set_file
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    blobs.set_file(&collection_name.clone(), &file_path.clone(), &file_hash).await.unwrap();

    // Test get_file
    let retrieved_file_hash = blobs.get_file(&collection_name.clone(), &file_path.clone()).await.unwrap();
    assert_eq!(file_hash, retrieved_file_hash, "The file hash should match the uploaded file hash");

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_delete_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs.create_collection(&collection_name.clone()).await.unwrap();

    // Test set_file
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    blobs.set_file(&collection_name.clone(), &file_path.clone(), &file_hash).await.unwrap();

    // Test delete_file
    let new_collection_hash = blobs.delete_file(&collection_name.clone(), &file_path.clone()).await.unwrap();
    assert!(!new_collection_hash.as_bytes().is_empty(), "New collection hash after deletion should not be empty");

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_list_files() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs.create_collection(&collection_name.clone()).await.unwrap();

    // Test set_file
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    blobs.set_file(&collection_name.clone(), &file_path.clone(), &file_hash).await.unwrap();

    // Test list_files
    let file_list = blobs.list_files(&collection_name.clone()).await.unwrap();
    assert_eq!(file_list.len(), 1, "There should be one file in the collection");
    assert_eq!(file_list[0], file_path, "The file path should match the uploaded file path");

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_collection_hash() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    let initial_collection_hash = blobs.create_collection(&collection_name.clone()).await.unwrap();

    // Test collection_hash
    let retrieved_collection_hash = blobs.collection_hash(&collection_name).await.unwrap();
    assert_eq!(initial_collection_hash, retrieved_collection_hash, "The retrieved collection hash should match the created collection hash");

    blobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn test_upload_to() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();

    // Test create_collection
    let collection_name = "my_test_collection".to_string();
    blobs.create_collection(&collection_name.clone()).await.unwrap();

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
    let file_hash = blobs.upload_to(&collection_name, &file_path, receiver).await.unwrap();

    // Add the uploaded file to the collection
    let new_file_collection_hash = blobs.set_file(&collection_name.clone(), &file_path.clone(), &file_hash).await.unwrap();
    assert!(!new_file_collection_hash.as_bytes().is_empty(), "New collection hash after uploading a file should not be empty");

    blobs.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_missing_collection() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();

    // Attempt to retrieve a file from a non-existent collection
    let result = blobs.get_file(&"non_existent_collection".to_string(), &"some_file.txt".to_string()).await;
    assert!(result.is_err(), "Retrieving file from non-existent collection should fail");

    // Attempt to list files in a non-existent collection
    let result = blobs.list_files(&"non_existent_collection".to_string()).await;
    assert!(result.is_err(), "Listing files from non-existent collection should fail");
}

#[tokio::test]
async fn test_overwrite_file() {
    let mut base_dir = PathBuf::new();
    base_dir.push(".veilid");

    let blobs = VeilidIrohBlobs::from_directory(&base_dir, None).await.unwrap();
    let collection_name = "my_test_collection".to_string();
    blobs.create_collection(&collection_name.clone()).await.unwrap();
    
    let file_path = "test_file.txt".to_string();
    let temp_file_path = base_dir.join("test_file.txt");
    std::fs::write(&temp_file_path, "test file content").unwrap();
    let temp_file_path = std::fs::canonicalize(temp_file_path).unwrap();

    let file_hash = blobs.upload_from_path(temp_file_path.clone()).await.unwrap();
    blobs.set_file(&collection_name.clone(), &file_path.clone(), &file_hash).await.unwrap();

    // Overwrite the file with new content
    std::fs::write(&temp_file_path, "new test file content").unwrap();
    let new_file_hash = blobs.upload_from_path(temp_file_path).await.unwrap();
    let updated_collection_hash = blobs.set_file(&collection_name.clone(), &file_path.clone(), &new_file_hash).await.unwrap();

    assert!(!updated_collection_hash.as_bytes().is_empty(), "Updated collection hash should not be empty");
    
    // Ensure that the file hash was updated
    let retrieved_file_hash = blobs.get_file(&collection_name.clone(), &file_path.clone()).await.unwrap();
    assert_eq!(new_file_hash, retrieved_file_hash, "The file hash should be updated after overwrite");
}

