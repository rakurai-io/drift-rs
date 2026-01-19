use std::sync::Arc;

use crate::{
    account_map::AccountMap,
    accounts::User,
    dlob::{DLOBEvent, DLOBNotifier, OrderDelta, DLOB},
    grpc::AccountUpdate,
    types::{MarketId, OrderStatus, DataAndSlot},
    DriftClient,
};
use anchor_lang::Discriminator;
use bytemuck::Pod;
use log::warn;
use solana_sdk::pubkey::Pubkey;

/// Trait for account maps that can provide User account data
pub trait AccountMapTrait: Send + Sync {
    fn account_data_and_slot<T: Pod>(&self, account: &Pubkey) -> Option<DataAndSlot<T>>;
}

// Implement trait for AccountMap
impl AccountMapTrait for AccountMap {
    fn account_data_and_slot<T: Pod>(&self, account: &Pubkey) -> Option<DataAndSlot<T>> {
        AccountMap::account_data_and_slot(self, account)
    }
}

// Implement trait for Arc<AccountMap>
impl AccountMapTrait for Arc<AccountMap> {
    fn account_data_and_slot<T: Pod>(&self, account: &Pubkey) -> Option<DataAndSlot<T>> {
        AccountMap::account_data_and_slot(self, account)
    }
}

// Blanket implementation for references to types implementing AccountMapTrait
impl<M: AccountMapTrait> AccountMapTrait for &M {
    fn account_data_and_slot<T: Pod>(&self, account: &Pubkey) -> Option<DataAndSlot<T>> {
        (**self).account_data_and_slot(account)
    }
}

/// Convenience builder for constructing and managing an event driven [`DLOB`] instance.
/// It should be plugged into a gRPC subscription to receive live order updates and slot changes.
///
/// ```example(no_run)
/// use drift_rs::dlob::builder::DLOBBuilder;
/// use drift_rs::types::MarketId;
///
/// // Define the markets you want to track
/// let market_ids = vec![MarketId::new(1), MarketId::new(2)];
///
/// // Construct the DLOBBuilder
/// let builder = DLOBBuilder::new(market_ids);
///
/// // setup grpc client...
///     let _res = drift
///     .grpc_subscribe(
///         grpc_url,
///         grpc_x_token,
///         GrpcSubscribeOpts::default()
///             .commitment(CommitmentLevel::Processed)
///             .usermap_on()
///             .on_user_account(builder.account_update_handler(drift.backend().account_map()))
///             .on_slot(builder.slot_update_handler(drift.clone())),
///         true, // sync all the accounts on startup (required to populate the usermap)
///     )
///    .await;
///
/// // Access the underlying DLOB
/// let dlob = builder.dlob();
/// ```
pub struct DLOBBuilder<'a> {
    dlob: &'a DLOB,
    notifier: DLOBNotifier,
    market_ids: Vec<MarketId>,
}

impl<'a> DLOBBuilder<'a> {
    /// Initialize a new DLOBBuilder instance
    pub fn new(market_ids: Vec<MarketId>) -> Self {
        let dlob = Box::leak(Box::new(DLOB::default()));
        let notifier = dlob.spawn_notifier();

        Self {
            dlob,
            notifier,
            market_ids,
        }
    }

    /// Return the DLOB instance
    pub fn dlob(&self) -> &'a DLOB {
        self.dlob
    }

    /// Returns a handler suitable for use in grpc_subscribe's on_account
    ///
    /// This will notify the DLOB of order changes based on User account updates
    pub fn account_update_handler<'b, M: AccountMapTrait>(
        &self,
        account_map: &'b M,
    ) -> impl Fn(&AccountUpdate) + Send + Sync + 'b {
        let notifier = self.notifier.clone();
        move |update| {
            // Validate account data size before deserializing to prevent panics
            let min_size = 8 + std::mem::size_of::<User>();
            if update.data.len() < min_size {
                warn!(
                    "User account data too small: expected at least {} bytes, got {} bytes for account {}",
                    min_size,
                    update.data.len(),
                    update.pubkey
                );
                return;
            }
            
            // Validate discriminator matches User account type
            if update.data.len() >= 8 && &update.data[..8] != User::DISCRIMINATOR {
                warn!(
                    "Account discriminator mismatch: expected User account for {}",
                    update.pubkey
                );
                return;
            }
            
            let new_user = crate::utils::deser_zero_copy(update.data);
            match account_map.account_data_and_slot::<User>(&update.pubkey) {
                Some(stored) => {
                    if stored.slot <= update.slot {
                        let user_order_deltas = crate::dlob::util::compare_user_orders(
                            update.pubkey,
                            &stored.data,
                            new_user,
                        );
                        for delta in user_order_deltas {
                            notifier
                                .send(DLOBEvent::Order {
                                    delta,
                                    slot: update.slot,
                                })
                                .expect("sent");
                        }
                    }
                }
                None => {
                    for order in new_user.orders {
                        if order.status == OrderStatus::Open
                            && order.base_asset_amount > order.base_asset_amount_filled
                        {
                            notifier
                                .send(DLOBEvent::Order {
                                    delta: OrderDelta::Create {
                                        user: update.pubkey,
                                        order,
                                    },
                                    slot: update.slot,
                                })
                                .expect("sent");
                        }
                    }
                }
            }
        }
    }

    /// Returns a handler suitable for use in grpc_subscribe's on_slot
    ///
    /// This will notify the DLOB of slot/price updates for the given markets and send the slot to slot_tx.
    pub fn slot_update_handler(&self, drift: DriftClient) -> impl Fn(u64) + Send + Sync + 'static {
        let notifier = self.notifier.clone();
        let market_ids = self.market_ids.clone();
        move |new_slot| {
            for market in &market_ids {
                if let Some(oracle_price) = drift.try_get_oracle_price_data_and_slot(*market) {
                    notifier
                        .send(DLOBEvent::SlotOrPriceUpdate {
                            slot: new_slot,
                            market_index: market.index(),
                            market_type: market.kind(),
                            oracle_price: oracle_price.data.price as u64,
                        })
                        .expect("sent");
                }
            }
        }
    }

    /// Returns a handler suitable for use with OracleMap (no RPC calls, uses cached oracle prices)
    ///
    /// This will notify the DLOB of slot/price updates for the given markets using cached oracle prices.
    /// No RPC calls are made - oracle prices must be synced beforehand.
    pub fn slot_update_handler_with_oracle_map(
        &self,
        oracle_map: Arc<crate::oraclemap::OracleMap>,
    ) -> impl Fn(u64) + Send + Sync + 'static {
        let notifier = self.notifier.clone();
        let market_ids = self.market_ids.clone();
        move |new_slot| {
            for market in &market_ids {
                if let Some(oracle_price) = oracle_map.get_by_market(market) {
                    notifier
                        .send(DLOBEvent::SlotOrPriceUpdate {
                            slot: new_slot,
                            market_index: market.index(),
                            market_type: market.kind(),
                            oracle_price: oracle_price.data.price as u64,
                        })
                        .expect("sent");
                }
            }
        }
    }
}
