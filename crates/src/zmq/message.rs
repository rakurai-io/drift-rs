//! Geyser ZMQ Message types with bincode deserialization

use {
    bincode,
    serde::{Deserialize, Serialize},
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        hash::Hash,
        message::v0::LoadedAddresses,
        pubkey::Pubkey,
        signature::Signature,
        transaction::Transaction,
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{InnerInstructions, Rewards, RewardsAndNumPartitions},
    std::{collections::HashSet, time::SystemTime},
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Message {
    Slot(MessageSlot),
    Account(Box<MessageAccount>),
    Transaction(Box<MessageTransaction>),
    Entry(MessageEntry),
    BlockMeta(MessageBlockMeta),
    Block(MessageBlock),
    AccAndTx(Box<MessageAccAndTx>),
    Tick(MessageTick),
}

impl Default for Message {
    fn default() -> Self {
        Message::Entry(MessageEntry::default())
    }
}

impl Message {
    pub fn get_slot(&self) -> u64 {
        match self {
            Self::Tick(msg) => msg.slot,
            Self::Slot(msg) => msg.slot,
            Self::Account(msg) => msg.slot,
            Self::Transaction(msg) => msg.slot,
            Self::Entry(msg) => msg.slot,
            Self::BlockMeta(msg) => msg.slot,
            Self::Block(msg) => msg.meta.slot,
            Self::AccAndTx(msg) => msg.slot,
        }
    }

    pub fn get_type(&self) -> &'static str {
        match self {
            Message::Tick(_) => "Tick",
            Message::Slot(_) => "Slot",
            Message::Account(_) => "Account",
            Message::Transaction(_) => "Transaction",
            Message::Entry(_) => "Entry",
            Message::BlockMeta(_) => "BlockMeta",
            Message::Block(_) => "Block",
            Message::AccAndTx(_) => "Acctx",
        }
    }

    pub fn get_type_enum(&self) -> MessageType {
        match self {
            Message::Tick(_) => MessageType::Tick,
            Message::Slot(_) => MessageType::Slot,
            Message::Account(_) => MessageType::Account,
            Message::Transaction(_) => MessageType::Transaction,
            Message::Entry(_) => MessageType::Entry,
            Message::BlockMeta(_) => MessageType::BlockMeta,
            Message::Block(_) => MessageType::Block,
            Message::AccAndTx(_) => MessageType::AccAndTx,
        }
    }

    /// Deserialize a bincode-encoded message from bytes
    pub fn from_bincode(bytes: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageType {
    Tick,
    Slot,
    Account,
    Transaction,
    Entry,
    BlockMeta,
    Block,
    AccAndTx,
}

impl MessageType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::Tick => "Tick",
            MessageType::Slot => "Slot",
            MessageType::Account => "Account",
            MessageType::Transaction => "Transaction",
            MessageType::Entry => "Entry",
            MessageType::BlockMeta => "BlockMeta",
            MessageType::Block => "Block",
            MessageType::AccAndTx => "AccAndTx",
        }
    }

    pub fn parse_variant(s: &str) -> Option<Self> {
        match s {
            "Slot" => Some(Self::Slot),
            "Account" => Some(Self::Account),
            "Transaction" => Some(Self::Transaction),
            "Entry" => Some(Self::Entry),
            "BlockMeta" => Some(Self::BlockMeta),
            "Block" => Some(Self::Block),
            "AccAndTx" => Some(Self::AccAndTx),
            "Tick" => Some(Self::Tick),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub txn_signature: Option<Signature>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageAccount {
    pub account: MessageAccountInfo,
    pub slot: Slot,
    pub is_startup: bool,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageSlot {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: SlotStatus,
    pub dead_error: Option<String>,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommitmentLevel {
    Processed,
    Confirmed,
    Finalized,
}

impl CommitmentLevel {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SlotStatus {
    Processed,
    Confirmed,
    Finalized,
    FirstShredReceived,
    Completed,
    CreatedBank,
    Dead,
}

impl PartialEq<SlotStatus> for CommitmentLevel {
    fn eq(&self, other: &SlotStatus) -> bool {
        match self {
            Self::Processed if *other == SlotStatus::Processed => true,
            Self::Confirmed if *other == SlotStatus::Confirmed => true,
            Self::Finalized if *other == SlotStatus::Finalized => true,
            _ => false,
        }
    }
}

impl SlotStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
            Self::FirstShredReceived => "first_shread_received",
            Self::Completed => "completed",
            Self::CreatedBank => "created_bank",
            Self::Dead => "dead",
        }
    }
}

// The standard TransactionStatusMeta does not impl Serialize/Deserialize traits
// Optimized with pre-allocation to reduce copying overhead
// Note: TransactionResult doesn't implement Serialize, so we use a custom serialization
#[derive(Clone, Debug, PartialEq)]
pub struct TransactionStatusMetaInfo {
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub rewards: Option<Rewards>,
    pub loaded_addresses: LoadedAddresses,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    // pub meta: TransactionStatusMetaInfo,
    pub transaction: Transaction,
    pub index: usize,
    pub account_keys: HashSet<Pubkey>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageTransaction {
    pub transaction: MessageTransactionInfo,
    pub slot: u64,
    pub created_at: SystemTime,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub struct MessageEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
    pub created_at: SystemTime,
}

impl Default for MessageEntry {
    fn default() -> Self {
        Self {
            slot: 0,
            index: 0,
            num_hashes: 0,
            hash: Hash::default(),
            executed_transaction_count: 0,
            starting_transaction_index: 0,
            created_at: SystemTime::now(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageBlockMeta {
    pub parent_slot: Slot,
    pub parent_blockhash: Hash,
    pub slot: Slot,
    pub blockhash: Hash,
    pub rewards: RewardsAndNumPartitions,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
    pub created_at: SystemTime,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageBlock {
    pub meta: MessageBlockMeta,
    pub transactions: Vec<MessageTransactionInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageAccAndTx {
    pub account: Vec<MessageAccount>,
    pub transaction: MessageTransaction,
    pub slot: u64,
    pub created_at: SystemTime,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TickSource {
    /// Tick generated from PohRecorder (periodic, real-time)
    PohRecorder = 0,
    /// Tick processed from BlockstoreProcessor (batched, from ledger)
    BlockstoreProcessor = 1,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct MessageTick {
    pub slot: Slot,
    pub tick_index: u64,
    pub num_hashes: u64,
    pub hash: Hash,
    pub leader: Option<Pubkey>,
    pub source: TickSource,
    pub created_at: SystemTime,
}
