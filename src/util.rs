use anyhow::{anyhow, Result};
use veilid_core::{PrivateSpec, RouteId, Sequencing, Stability, VeilidAPI, VALID_CRYPTO_KINDS};

pub async fn make_route(veilid: &VeilidAPI) -> Result<(RouteId, Vec<u8>)> {
    let mut retries = 3;
    while retries != 0 {
        retries -= 1;
        let result = veilid
            .new_custom_private_route(PrivateSpec {
                crypto_kinds: VALID_CRYPTO_KINDS.to_vec(),
                hop_count: 0,
                stability: Stability::LowLatency,
                sequencing: Sequencing::NoPreference,
            })
            .await;

        if let Ok(route_blob) = result {
            return Ok((route_blob.route_id, route_blob.blob));
        }
    }
    Err(anyhow!("Unable to create route, reached max retries"))
}
