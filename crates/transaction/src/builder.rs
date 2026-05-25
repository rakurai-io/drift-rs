//! Transaction builder utilities for Drift Protocol

use drift_rs::{
    types::{MarketId, MarketType, OrderParams, PositionDirection},
    DriftClient, SdkResult, TransactionBuilder,
};
use solana_sdk::pubkey::Pubkey;

/// Helper functions for building perp transactions
pub struct PerpTransactionBuilder;

impl PerpTransactionBuilder {
    /// Create a simple perp order
    ///
    /// # Arguments
    /// * `market_index` - The perp market index
    /// * `direction` - Long or Short position direction
    /// * `base_asset_amount` - The base asset amount (in native units, e.g., for SOL use 1_000_000_000 for 1 SOL)
    /// * `price` - The limit price (0 for market orders)
    ///
    /// # Returns
    /// An `OrderParams` struct ready to be used in a transaction
    pub fn create_perp_order(
        market_index: u16,
        direction: PositionDirection,
        base_asset_amount: u64,
        price: u64,
    ) -> OrderParams {
        OrderParams {
            market_index,
            market_type: MarketType::Perp,
            direction,
            base_asset_amount,
            price,
            ..Default::default()
        }
    }

    /// Build and send a perp order transaction
    ///
    /// # Arguments
    /// * `client` - The DriftClient instance
    /// * `sub_account` - The user's sub-account pubkey
    /// * `order` - The order parameters
    ///
    /// # Returns
    /// The transaction signature on success
    pub async fn place_perp_order(
        client: &DriftClient,
        sub_account: &Pubkey,
        order: OrderParams,
    ) -> SdkResult<solana_sdk::signature::Signature> {
        let mut tx_builder = client.init_tx(sub_account, false).await?;
        
        tx_builder = tx_builder.place_orders(vec![order]);
        
        let tx = tx_builder.build();
        client.sign_and_send(tx).await
    }

    /// Build a perp order transaction without sending
    ///
    /// # Arguments
    /// * `client` - The DriftClient instance
    /// * `sub_account` - The user's sub-account pubkey
    /// * `order` - The order parameters
    ///
    /// # Returns
    /// A built transaction message ready to be signed and sent
    pub async fn build_perp_order_tx(
        client: &DriftClient,
        sub_account: &Pubkey,
        order: OrderParams,
    ) -> SdkResult<solana_sdk::message::VersionedMessage> {
        let mut tx_builder = client.init_tx(sub_account, false).await?;
        
        tx_builder = tx_builder.place_orders(vec![order]);
        
        Ok(tx_builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_perp_order() {
        let order = PerpTransactionBuilder::create_perp_order(
            0, // SOL-PERP market
            PositionDirection::Long,
            1_000_000_000, // 1 SOL
            0, // Market order
        );

        assert_eq!(order.market_index, 0);
        assert_eq!(order.market_type, MarketType::Perp);
        assert_eq!(order.direction, PositionDirection::Long);
        assert_eq!(order.base_asset_amount, 1_000_000_000);
    }
}

