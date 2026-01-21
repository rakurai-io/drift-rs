use std::time::Duration;

use ahash::HashSet;
use futures_util::StreamExt;
use log::{error, info};
use solana_rpc_client_api::filter::Memcmp;
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use zmq::{Context, Socket, SocketType};

use crate::grpc::grpc_subscriber::GrpcError;
use crate::grpc::AccountUpdate;
use crate::types::UnsubHandle;

use super::message::Message;
use super::OnAccountFn;

type Hooks = Vec<(AccountFilter, Box<OnAccountFn>)>;

/// Provides filter criteria for accounts over ZMQ
///
/// There are two filter modes:
///
/// * `full` - requires all filters to trigger a match
/// * `partial` - any one filter will trigger a match
#[derive(Clone, Default, Debug)]
pub struct AccountFilter {
    /// optionally filter updates by discriminator
    discriminator: Option<&'static [u8]>,
    /// optionally filter updates by Solana Memcmp matches
    memcmp: Option<Memcmp>,
    /// optionally filter updates by pubkey
    accounts: Option<HashSet<Pubkey>>,
    /// true = full match mode, false = partial
    is_full: bool,
}

impl AccountFilter {
    /// Create a filter that matches ALL accounts!
    pub fn firehose() -> Self {
        AccountFilter::full()
    }
    /// Create a filter that matches iff all parameters are satisfied
    pub fn full() -> Self {
        AccountFilter {
            is_full: true,
            ..Default::default()
        }
    }
    /// Create a filter that matches when any criteria is satisfied
    pub fn partial() -> Self {
        AccountFilter {
            is_full: false,
            ..Default::default()
        }
    }
    /// add filter for given `pubkeys`
    pub fn with_accounts(mut self, pubkeys: impl Iterator<Item = Pubkey>) -> Self {
        self.accounts = Some(ahash::HashSet::from_iter(pubkeys));
        self
    }
    /// add filter for given anchor account `discriminator`
    pub fn with_discriminator(mut self, discriminator: &'static [u8]) -> Self {
        self.discriminator = Some(discriminator);
        self
    }
    /// add filter for given memcmp filter
    pub fn with_memcmp(mut self, memcmp: Memcmp) -> Self {
        self.memcmp = Some(memcmp);
        self
    }
    /// Returns true if pubkey/account matches the filter
    pub fn matches(&self, pubkey: &Pubkey, account_data: &[u8]) -> bool {
        if !self.is_full {
            self.discriminator
                .is_some_and(|x| account_data.len() >= 8 && x == &account_data[..8])
                || self.accounts.as_ref().is_some_and(|x| x.contains(pubkey))
                || self
                    .memcmp
                    .as_ref()
                    .is_some_and(|x| x.bytes_match(account_data))
        } else {
            (match self.discriminator {
                Some(x) => account_data.len() >= 8 && x == &account_data[..8],
                None => true,
            }) && (match self.accounts.as_ref() {
                Some(x) => x.contains(pubkey),
                None => true,
            }) && (match self.memcmp.as_ref() {
                Some(x) => x.bytes_match(account_data),
                None => true,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZmqConnectionOpts {
    /// ZMQ socket type (default: SUB)
    pub socket_type: SocketType,
    /// ZMQ receive timeout in milliseconds
    pub receive_timeout_ms: Option<i32>,
    /// ZMQ send timeout in milliseconds
    pub send_timeout_ms: Option<i32>,
    /// ZMQ receive high water mark
    pub receive_hwm: Option<i32>,
    /// ZMQ send high water mark
    pub send_hwm: Option<i32>,
    /// ZMQ linger period in milliseconds
    pub linger_ms: Option<i32>,
    /// ZMQ reconnect interval in milliseconds
    pub reconnect_interval_ms: Option<i32>,
    /// ZMQ max reconnect interval in milliseconds
    pub max_reconnect_interval_ms: Option<i32>,
}

impl Default for ZmqConnectionOpts {
    fn default() -> Self {
        Self {
            socket_type: SocketType::SUB,
            receive_timeout_ms: Some(1000),
            send_timeout_ms: None,
            receive_hwm: Some(1000),
            send_hwm: None,
            linger_ms: Some(0),
            reconnect_interval_ms: Some(100),
            max_reconnect_interval_ms: Some(10000),
        }
    }
}

#[derive(Debug, thiserror::Error)]
/// drift ZMQ error
pub enum ZmqError {
    #[error("zmq context err: {0}")]
    Context(String),
    #[error("zmq socket err: {0}")]
    Socket(String),
    #[error("zmq connect err: {0}")]
    Connect(String),
    #[error("zmq receive err: {0}")]
    Receive(String),
    #[error("zmq deserialize err: {0}")]
    Deserialize(String),
}

/// specialized Drift ZMQ client
pub struct DriftZmqClient {
    account_endpoint: String,
    slot_endpoint: String,
    zmq_opts: Option<ZmqConnectionOpts>,
    on_account_hooks: Hooks,
    on_slot: Box<dyn Fn(Slot) + Send + Sync + 'static>,
}

impl DriftZmqClient {
    /// Create a new `DriftZmqClient`
    ///
    /// It can be started by calling `subscribe`
    pub fn new(account_endpoint: String, slot_endpoint: String) -> Self {
        Self {
            account_endpoint,
            slot_endpoint,
            on_account_hooks: Default::default(),
            zmq_opts: None,
            on_slot: Box::new(move |_slot| {}),
        }
    }//call this

    /// Set ZMQ network options
    pub fn zmq_connection_opts(mut self, zmq_opts: ZmqConnectionOpts) -> Self {
        let _ = self.zmq_opts.insert(zmq_opts);
        self
    }

    /// Add a callback on slot updates
    ///
    /// `on_slot` must prioritize fast handling or risk blocking the ZMQ thread
    pub fn on_slot<F: Fn(Slot) + Send + Sync + 'static>(&mut self, on_slot: F) {
        self.on_slot = Box::new(on_slot);
    }

    /// Add a callback for all account updates matching `filter`
    ///
    /// This may be called many times to define multiple callbacks
    ///
    /// * `filter` - filter accounts by criteria
    /// * `on_account` - fn to receive callback on filter match
    ///
    /// DEV: `on_account` must prioritize fast handling or risk blocking the ZMQ thread
    pub fn on_account<T: Fn(&AccountUpdate) + Send + Sync + 'static>(
        &mut self,
        filter: AccountFilter,
        on_account: T,
    ) {
        self.on_account_hooks.push((filter, Box::new(on_account)));
    }

    /// Start subscription for geyser updates
    ///
    /// Returns an unsub handle on success
    pub async fn subscribe(self) -> Result<UnsubHandle, GrpcError> {
        let opts = self.zmq_opts.clone().unwrap_or_default();
        let account_endpoint = self.account_endpoint.clone();
        let slot_endpoint = self.slot_endpoint.clone();
        let on_account_hooks = self.on_account_hooks;
        let on_slot = self.on_slot;

        let (unsub_tx, mut unsub_rx) = tokio::sync::oneshot::channel::<()>();

        // ZMQ receives updates very frequently, don't want tokio scheduler moving it
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let ls = tokio::task::LocalSet::new();
            let zmq_task = ls.spawn_local(Self::zmq_subscribe(
                account_endpoint,
                slot_endpoint,
                opts,
                on_account_hooks,
                on_slot,
            ));
            let mut waiter = futures_util::stream::FuturesUnordered::new();
            waiter.push(zmq_task);

            ls.block_on(&rt, async move {
                tokio::select! {
                    biased;
                    _ = &mut unsub_rx => (),
                    res = waiter.next() => {
                        if let Some(Ok(Some(err))) = res {
                            log::error!(target: "zmq", "subscription task failed: {err:?}");
                        } else {
                            log::error!(target: "zmq", "subscription task ended unexpectedly");
                        }
                    }
                }
            });
            info!(target: "zmq", "ZMQ connection unsubscribed");
        });

        info!(target: "zmq", "ZMQ subscribed ⚡️");
        Ok(unsub_tx)
    }

    /// Run the ZMQ subscription task
    ///
    /// It receives all configured updates and routes them to registered callbacks
    async fn zmq_subscribe(
        account_endpoint: String,
        slot_endpoint: String,
        opts: ZmqConnectionOpts,
        on_account: Hooks,
        on_slot: impl Fn(Slot),
    ) -> Option<ZmqError> {
        let max_retries = 3;
        let mut retry_count = 0;
        let mut latest_slot = 0;
        let mut last_error: Option<ZmqError> = None;
        loop {
            if retry_count >= max_retries {
                log::warn!(target: "zmq", "max retry attempts reached. disconnecting...");
                break;
            }

            // Create ZMQ context
            let ctx = Context::new();

            // Create and configure account socket
            let account_socket = match Self::create_socket(&ctx, &opts) {
                Ok(socket) => socket,
                Err(err) => {
                    log::warn!(target: "zmq", "failed to create account socket: {err:?}");
                    retry_count += 1;
                    tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count + 1))).await;
                    let _ = last_error.insert(err);
                    continue;
                }
            };

            // Create and configure slot socket
            let slot_socket = match Self::create_socket(&ctx, &opts) {
                Ok(socket) => socket,
                Err(err) => {
                    log::warn!(target: "zmq", "failed to create slot socket: {err:?}");
                    retry_count += 1;
                    tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count + 1))).await;
                    let _ = last_error.insert(err);
                    continue;
                }
            };

            // Connect sockets
            if let Err(err) = account_socket.connect(&account_endpoint) {
                log::warn!(target: "zmq", "failed to connect account socket: {err:?}");
                retry_count += 1;
                tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count + 1))).await;
                let _ = last_error.insert(ZmqError::Connect(err.to_string()));
                continue;
            }

            if let Err(err) = slot_socket.connect(&slot_endpoint) {
                log::warn!(target: "zmq", "failed to connect slot socket: {err:?}");
                retry_count += 1;
                tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count + 1))).await;
                let _ = last_error.insert(ZmqError::Connect(err.to_string()));
                continue;
            }

            // Subscribe to all messages (empty filter means all)
            if let Err(err) = account_socket.set_subscribe(b"") {
                log::warn!(target: "zmq", "failed to subscribe account socket: {err:?}");
                retry_count += 1;
                tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count + 1))).await;
                let _ = last_error.insert(ZmqError::Socket(err.to_string()));
                continue;
            }

            if let Err(err) = slot_socket.set_subscribe(b"") {
                log::warn!(target: "zmq", "failed to subscribe slot socket: {err:?}");
                retry_count += 1;
                tokio::time::sleep(Duration::from_secs(2_u64.pow(retry_count + 1))).await;
                let _ = last_error.insert(ZmqError::Socket(err.to_string()));
                continue;
            }

            info!(target: "zmq", "ZMQ connected 🔌: account={account_endpoint}, slot={slot_endpoint}");
            retry_count = 0;

            // Poll both sockets in a loop
            loop {
                // Try to receive from account socket (non-blocking)
                match Self::receive_and_process_account(
                    &account_socket,
                    &on_account,
                    &mut latest_slot,
                ) {
                    Ok(()) => {}
                    Err(ZmqError::Receive(_)) => {
                        // No message available, continue
                    }
                    Err(err) => {
                        log::warn!(target: "zmq", "account socket error: {err:?}");
                        let _ = last_error.insert(err);
                        break;
                    }
                }

                // Try to receive from slot socket (non-blocking)
                match Self::receive_and_process_slot(&slot_socket, &on_slot, &mut latest_slot) {
                    Ok(()) => {}
                    Err(ZmqError::Receive(_)) => {
                        // No message available, continue
                    }
                    Err(err) => {
                        log::warn!(target: "zmq", "slot socket error: {err:?}");
                        let _ = last_error.insert(err);
                        break;
                    }
                }

                // Small delay to prevent busy waiting
                // tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        error!(target: "zmq", "ZMQ stream closed");
        last_error
    }

    fn create_socket(ctx: &Context, opts: &ZmqConnectionOpts) -> Result<Socket, ZmqError> {
        let socket = ctx
            .socket(opts.socket_type)
            .map_err(|e| ZmqError::Socket(e.to_string()))?;

        if let Some(timeout) = opts.receive_timeout_ms {
            socket
                .set_rcvtimeo(timeout)
                .map_err(|e| ZmqError::Socket(e.to_string()))?;
        }

        if let Some(timeout) = opts.send_timeout_ms {
            socket
                .set_sndtimeo(timeout)
                .map_err(|e| ZmqError::Socket(e.to_string()))?;
        }

        if let Some(hwm) = opts.receive_hwm {
            socket
                .set_rcvhwm(hwm)
                .map_err(|e| ZmqError::Socket(e.to_string()))?;
        }

        if let Some(hwm) = opts.send_hwm {
            socket
                .set_sndhwm(hwm)
                .map_err(|e| ZmqError::Socket(e.to_string()))?;
        }

        if let Some(linger) = opts.linger_ms {
            socket
                .set_linger(linger)
                .map_err(|e| ZmqError::Socket(e.to_string()))?;
        }

        if let Some(interval) = opts.reconnect_interval_ms {
            socket
                .set_reconnect_ivl(interval)
                .map_err(|e| ZmqError::Socket(e.to_string()))?;
        }

        if let Some(max_interval) = opts.max_reconnect_interval_ms {
            socket
                .set_reconnect_ivl_max(max_interval)
                .map_err(|e| ZmqError::Socket(e.to_string()))?;
        }

        Ok(socket)
    }

    fn receive_and_process_account(
        socket: &Socket,
        on_account: &Hooks,
        latest_slot: &mut Slot,
    ) -> Result<(), ZmqError> {
        // ZMQ SUB sockets send messages in multipart format: [topic, data]
        // We'll receive both parts
        let mut msg = zmq::Message::new();
        socket
            .recv(&mut msg, zmq::DONTWAIT)
            .map_err(|e| ZmqError::Receive(e.to_string()))?;

        // Get the data part (second message)
        let mut data_msg = zmq::Message::new();
        socket
            .recv(&mut data_msg, zmq::DONTWAIT)
            .map_err(|e| ZmqError::Receive(e.to_string()))?;

        let bytes = data_msg.as_ref();

        // Deserialize bincode message
        let message =
            Message::from_bincode(bytes).map_err(|e| ZmqError::Deserialize(e.to_string()))?;

        match message {
            Message::Account(account_msg) => {
                let account = &account_msg.account;
                let pubkey = account.pubkey;
                let slot = account_msg.slot;

                if slot > *latest_slot {
                    *latest_slot = slot;
                }
                // Convert MessageAccount to GRPC AccountUpdate
                let data_vec = account.data.clone();
                let grpc_update = AccountUpdate {
                    owner: account.owner,
                    pubkey,
                    slot,
                    lamports: account.lamports,
                    executable: account.executable,
                    rent_epoch: account.rent_epoch,
                    data: data_vec.as_slice(),
                };
                for (filter, hook) in on_account {
                    if filter.matches(&pubkey, &account.data) {
                        hook(&grpc_update);
                    }
                }
            }
            _ => {
                println!(
                    "zmq: ignoring non-account message type on account socket: {}",
                    message.get_type()
                );
            }
        }

        Ok(())
    }

    fn receive_and_process_slot(
        socket: &Socket,
        on_slot: &impl Fn(Slot),
        latest_slot: &mut Slot,
    ) -> Result<(), ZmqError> {
        // ZMQ SUB sockets send messages in multipart format: [topic, data]
        let mut msg = zmq::Message::new();
        socket
            .recv(&mut msg, zmq::DONTWAIT)
            .map_err(|e| ZmqError::Receive(e.to_string()))?;

        // Get the data part (second message)
        let mut data_msg = zmq::Message::new();
        socket
            .recv(&mut data_msg, zmq::DONTWAIT)
            .map_err(|e| ZmqError::Receive(e.to_string()))?;

        let bytes = data_msg.as_ref();

        // Deserialize bincode message
        let message =
            Message::from_bincode(bytes).map_err(|e| ZmqError::Deserialize(e.to_string()))?;

        match message {
            Message::Slot(slot_msg) => {
                let slot = slot_msg.slot;
                log::trace!(target: "zmq", "slot: {slot}");
                if slot > *latest_slot {
                    *latest_slot = slot;
                    on_slot(slot);
                }
            }
            _ => {
                log::trace!(target: "zmq", "ignoring non-slot message type on slot socket: {}", message.get_type());
            }
        }

        Ok(())
    }
}
