use jsonrpsee::proc_macros::rpc;
use reth_rpc_types::pubsub::{Kind, Params};

/// Ethereum pub-sub rpc interface.
#[cfg_attr(not(feature = "client"), rpc(server))]
#[cfg_attr(feature = "client", rpc(server, client))]
pub trait EthPubSubApi {
    /// Create an ethereum subscription.
    #[subscription(
        name = "eth_subscribe" => "eth_subscription",
        unsubscribe = "eth_unsubscribe",
        item = reth_rpc_types::pubsub::SubscriptionResult
    )]
    fn eth_subscribe(&self, kind: Kind, params: Option<Params>);
}