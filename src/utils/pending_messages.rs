use std::collections::hash_map;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tycho_types::cell::HashBytes;
use tycho_types::models::ShardIdent;
use tycho_util::FastHashMap;

#[derive(Default, Clone)]
pub struct PendingMessages {
    inner: Arc<Mutex<PendingMessagesInner>>,
}

impl PendingMessages {
    pub fn add_message(
        &self,
        account: HashBytes,
        message_hash: HashBytes,
        expire_at: u32,
    ) -> anyhow::Result<MessageStatusRx> {
        let mut inner = self.inner.lock();

        match inner.entries.entry(PendingMessageId {
            account,
            message_hash,
        }) {
            hash_map::Entry::Vacant(entry) => {
                let (tx, rx) = oneshot::channel();
                entry.insert(PendingMessage {
                    tx: Some(tx),
                    expire_at,
                });

                inner.min_expire_at = inner.min_expire_at.min(expire_at);

                Ok(rx)
            }
            hash_map::Entry::Occupied(_) => Err(PendingMessagesQueueError::AlreadyExists.into()),
        }
    }

    pub fn deliver_message(&self, account: HashBytes, message_hash: HashBytes) {
        let mut inner = self.inner.lock();

        let Some(mut message) = inner.entries.remove(&PendingMessageId {
            account,
            message_hash,
        }) else {
            return;
        };

        if let Some(tx) = message.tx.take() {
            tx.send(MessageStatus::Delivered).ok();
        }

        let current_min_expire_at = inner.min_expire_at;
        if current_min_expire_at != message.expire_at {
            return;
        }

        let mut min_expire_at = u32::MAX;
        inner.entries.iter().for_each(|(_, item)| {
            if item.expire_at < min_expire_at {
                min_expire_at = item.expire_at;
            }
        });

        inner.min_expire_at = min_expire_at;
    }

    pub fn update(&self, shard: &ShardIdent, current_utime: u32) {
        let mut inner = self.inner.lock();

        if current_utime <= inner.min_expire_at {
            return;
        }

        let mut min_expire_at = u32::MAX;

        inner.entries.retain(|id, item| {
            if current_utime <= item.expire_at || !shard.contains_account(&id.account) {
                if item.expire_at < min_expire_at {
                    min_expire_at = item.expire_at;
                }
                return true;
            }

            if let Some(tx) = item.tx.take() {
                tx.send(MessageStatus::Expired).ok();
            }

            false
        });

        inner.min_expire_at = min_expire_at;
    }
}

struct PendingMessagesInner {
    entries: FastHashMap<PendingMessageId, PendingMessage>,
    min_expire_at: u32,
}

impl Default for PendingMessagesInner {
    fn default() -> Self {
        Self {
            entries: FastHashMap::default(),
            min_expire_at: u32::MAX,
        }
    }
}

pub type MessageStatusRx = oneshot::Receiver<MessageStatus>;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct PendingMessageId {
    account: HashBytes,
    message_hash: HashBytes,
}

struct PendingMessage {
    tx: Option<oneshot::Sender<MessageStatus>>,
    expire_at: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MessageStatus {
    Delivered,
    Expired,
}

#[derive(thiserror::Error, Debug)]
enum PendingMessagesQueueError {
    #[error("Already exists")]
    AlreadyExists,
}
