/// Hand-written protobuf wire message types.
///
/// Generated `prost` output can replace this file when proto sources are
/// available. Fields keep the same wire encoding.
use prost::Message;

// -- consensus.proto --

#[derive(Clone, PartialEq, Eq, Message)]
pub struct XtId {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct TransactionRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub chain_id: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub transaction: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct XtRequest {
    #[prost(message, repeated, tag = "1")]
    pub transactions: Vec<TransactionRequest>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Vote {
    #[prost(bytes = "vec", tag = "1")]
    pub sender_chain_id: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub xt_id: Option<XtId>,
    #[prost(bool, tag = "3")]
    pub vote: bool,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Decided {
    #[prost(message, optional, tag = "1")]
    pub xt_id: Option<XtId>,
    #[prost(bool, tag = "2")]
    pub decision: bool,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct CircMessage {
    #[prost(bytes = "vec", tag = "1")]
    pub source_chain: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub destination_chain: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub source: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub receiver: Vec<Vec<u8>>,
    #[prost(message, optional, tag = "5")]
    pub xt_id: Option<XtId>,
    #[prost(string, tag = "6")]
    pub label: String,
    #[prost(bytes = "vec", repeated, tag = "7")]
    pub data: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "8")]
    pub session_id: Vec<u8>,
}

// -- sbcp.proto --

#[derive(Clone, PartialEq, Eq, Message)]
pub struct L2BlockRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub chain_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub block_number: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub parent_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StartSlot {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(uint64, tag = "2")]
    pub next_superblock_number: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub last_superblock_hash: Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    pub l2_blocks_request: Vec<L2BlockRequest>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct RequestSeal {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub included_xts: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct RollBackAndStartSlot {
    #[prost(message, repeated, tag = "1")]
    pub l2_blocks_request: Vec<L2BlockRequest>,
    #[prost(uint64, tag = "2")]
    pub current_slot: u64,
    #[prost(uint64, tag = "3")]
    pub next_superblock_number: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub last_superblock_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct L2Block {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub chain_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub block_number: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub block_hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub parent_block_hash: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "6")]
    pub included_xts: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "7")]
    pub block: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StartSc {
    #[prost(uint64, tag = "1")]
    pub slot: u64,
    #[prost(uint64, tag = "2")]
    pub xt_sequence_number: u64,
    #[prost(message, optional, tag = "3")]
    pub xt_request: Option<XtRequest>,
    #[prost(bytes = "vec", tag = "4")]
    pub xt_id: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Block {
    #[prost(bytes = "vec", tag = "1")]
    pub chain_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub block_data: Vec<u8>,
    #[prost(message, repeated, tag = "3")]
    pub included_xt_ids: Vec<XtId>,
}

// -- transport.proto --

#[derive(Clone, PartialEq, Eq, Message)]
pub struct HandshakeRequest {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(bytes = "vec", tag = "2")]
    pub public_key: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub signature: Vec<u8>,
    #[prost(string, tag = "4")]
    pub client_id: String,
    #[prost(bytes = "vec", tag = "5")]
    pub nonce: Vec<u8>,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct HandshakeResponse {
    #[prost(bool, tag = "1")]
    pub accepted: bool,
    #[prost(string, tag = "2")]
    pub error: String,
    #[prost(string, tag = "3")]
    pub session_id: String,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Ping {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Pong {
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct DisconnectMessage {
    #[prost(enumeration = "disconnect_message::Reason", tag = "1")]
    pub reason: i32,
    #[prost(string, tag = "2")]
    pub details: String,
}

pub mod disconnect_message {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
    #[repr(i32)]
    pub enum Reason {
        #[default]
        Requested = 0,
        ProtocolError = 1,
        TooManyPeers = 2,
        AuthFailure = 3,
        ReadTimeout = 4,
        WriteTimeout = 5,
        NetworkError = 6,
        InternalError = 7,
        Throttled = 8,
    }

    impl Reason {
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Self::Requested => "REQUESTED",
                Self::ProtocolError => "PROTOCOL_ERROR",
                Self::TooManyPeers => "TOO_MANY_PEERS",
                Self::AuthFailure => "AUTH_FAILURE",
                Self::ReadTimeout => "READ_TIMEOUT",
                Self::WriteTimeout => "WRITE_TIMEOUT",
                Self::NetworkError => "NETWORK_ERROR",
                Self::InternalError => "INTERNAL_ERROR",
                Self::Throttled => "THROTTLED",
            }
        }
    }

    impl From<i32> for Reason {
        fn from(value: i32) -> Self {
            match value {
                0 => Self::Requested,
                1 => Self::ProtocolError,
                2 => Self::TooManyPeers,
                3 => Self::AuthFailure,
                4 => Self::ReadTimeout,
                5 => Self::WriteTimeout,
                6 => Self::NetworkError,
                7 => Self::InternalError,
                8 => Self::Throttled,
                _ => Self::Requested,
            }
        }
    }
}

// -- messages.proto --

/// Sidecar-relevant protocol messages.
///
/// The publisher uses `StartSlot`/`RequestSeal`/`StartSC` from the SBCP layer.
/// The sidecar receives `StartInstance`, `Decided`, `MailboxMessage`, and the
/// rollback variant. These are re-mapped from the publisher's QUIC protocol.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct StartInstance {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub period_id: u64,
    #[prost(uint64, tag = "3")]
    pub sequence_number: u64,
    #[prost(message, optional, tag = "4")]
    pub xt_request: Option<XtRequest>,
}

impl StartInstance {
    /// Return the hex-encoded instance ID.
    pub fn instance_id_hex(&self) -> String {
        hex::encode(&self.instance_id)
    }
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct MailboxMessage {
    #[prost(bytes = "vec", tag = "1")]
    pub instance_id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub source_chain: u64,
    #[prost(uint64, tag = "3")]
    pub destination_chain: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub source: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub receiver: Vec<u8>,
    #[prost(string, tag = "6")]
    pub label: String,
    #[prost(bytes = "vec", repeated, tag = "7")]
    pub data: Vec<Vec<u8>>,
    #[prost(uint64, tag = "8")]
    pub session_id: u64,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct StartPeriod {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub superblock_number: u64,
}

#[derive(Clone, PartialEq, Eq, Message)]
pub struct Rollback {
    #[prost(uint64, tag = "1")]
    pub period_id: u64,
    #[prost(uint64, tag = "2")]
    pub last_finalized_superblock_num: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub last_finalized_superblock_hash: Vec<u8>,
}

/// Top-level message envelope containing the oneof `payload` from `messages.proto`.
///
/// Only includes the subset of message types that the sidecar handles.
#[derive(Clone, PartialEq, Eq, Message)]
pub struct WireMessage {
    #[prost(string, tag = "1")]
    pub sender_id: String,
    #[prost(
        oneof = "wire_message::Payload",
        tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20"
    )]
    pub payload: Option<wire_message::Payload>,
}

pub mod wire_message {
    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum Payload {
        // SCP messages
        #[prost(message, tag = "2")]
        XtRequest(super::XtRequest),
        #[prost(message, tag = "3")]
        Vote(super::Vote),
        #[prost(message, tag = "4")]
        Decided(super::Decided),
        #[prost(message, tag = "5")]
        Block(super::Block),
        #[prost(message, tag = "6")]
        CircMessage(super::CircMessage),

        // SBCP messages
        #[prost(message, tag = "7")]
        StartSlot(super::StartSlot),
        #[prost(message, tag = "8")]
        RequestSeal(super::RequestSeal),
        #[prost(message, tag = "9")]
        RollBackAndStartSlot(super::RollBackAndStartSlot),
        #[prost(message, tag = "10")]
        L2Block(super::L2Block),
        #[prost(message, tag = "11")]
        StartSc(super::StartSc),

        // Transport messages
        #[prost(message, tag = "12")]
        HandshakeRequest(super::HandshakeRequest),
        #[prost(message, tag = "13")]
        HandshakeResponse(super::HandshakeResponse),
        #[prost(message, tag = "14")]
        Ping(super::Ping),
        #[prost(message, tag = "15")]
        Pong(super::Pong),
        #[prost(message, tag = "16")]
        Disconnect(super::DisconnectMessage),

        // Sidecar control-plane messages
        #[prost(message, tag = "17")]
        StartInstance(super::StartInstance),
        #[prost(message, tag = "18")]
        StartPeriod(super::StartPeriod),
        #[prost(message, tag = "19")]
        MailboxMessage(super::MailboxMessage),
        #[prost(message, tag = "20")]
        Rollback(super::Rollback),
    }
}
