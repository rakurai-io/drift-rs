//! Example DLOB subscription/builder with ZMQ Geyser
use std::time::Duration;

use drift_rs::{dlob::builder::DLOBBuilder, types::MarketType, Context, DriftClient, RpcClient};
use solana_commitment_config::CommitmentLevel;
use solana_keypair::Keypair;

#[tokio::main]
async fn main() {
    let _ = dotenv::dotenv();

    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    let drift = DriftClient::new(
        Context::MainNet,
        RpcClient::new(rpc_url),
        Keypair::new().into(),
    )
    .await
    .expect("initialized client");
    let markets = drift.get_all_perp_market_ids();
    println!("subscribing to DLOB for markets: {:?}", markets);
    let dlob_builder = DLOBBuilder::new(markets);

    let zmq_account_endpoint = std::env::var("ZMQ_ACCOUNT_ENDPOINT")
        .unwrap_or_else(|_| "ipc:///tmp/account.sock".to_string());
    let zmq_slot_endpoint =
        std::env::var("ZMQ_SLOT_ENDPOINT").unwrap_or_else(|_| "ipc:///tmp/slot.sock".to_string());

    let _res = drift
        .zmq_subscribe(
            zmq_account_endpoint,
            zmq_slot_endpoint,
            drift_rs::zmq::ZmqSubscribeOpts::default()
                .commitment(CommitmentLevel::Processed)
                .usermap_on()
                .on_user_account(dlob_builder.account_update_handler(drift.backend().account_map()))
                .on_slot(dlob_builder.slot_update_handler(drift.clone())),
            true, // sync all the accounts on startup (required to populate the usermap)
        )
        .await;

    let dlob = dlob_builder.dlob();

    println!("printing sol-perp orderbook");
    for _ in 0..5 {
        println!("{}", dlob.get_l2_snapshot(0, MarketType::Perp));
        let _ = tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!("example finished");
}
