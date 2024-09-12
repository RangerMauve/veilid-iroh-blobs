use anyhow::anyhow;
use anyhow::Result;
use core::str;
use std::result;
use tunnels::OnNewTunnelCallback;
// use iroh_blobs::store::fs::Store;
use std::{path::PathBuf, sync::Arc};
// use tmpdir::TmpDir;
use tokio::time::{sleep, Duration};
use tokio::{
    join,
    sync::{
        broadcast,
        mpsc::{self, Receiver},
    },
};
use tracing::info;
use veilid_core::{
    RouteId, RoutingContext, UpdateCallback, VeilidAPI, VeilidConfigInner, VeilidUpdate,
    VALID_CRYPTO_KINDS,
};

mod tunnels;

use crate::tunnels::TunnelManager;

fn main() {
    println!("Hello, world!");
}
