//! Receiver-side counters. Atomic so recv workers and observability threads
//! can share the handle without a mutex.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct ReceiverStats {
    packets_recv: AtomicU64,
    bytes_recv: AtomicU64,
}

impl ReceiverStats {
    pub fn record_packet(&self, bytes: usize) {
        self.packets_recv.fetch_add(1, Ordering::Relaxed);
        self.bytes_recv.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> ReceiverStatsSnapshot {
        ReceiverStatsSnapshot {
            packets_recv: self.packets_recv.load(Ordering::Relaxed),
            bytes_recv: self.bytes_recv.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiverStatsSnapshot {
    pub packets_recv: u64,
    pub bytes_recv: u64,
}
