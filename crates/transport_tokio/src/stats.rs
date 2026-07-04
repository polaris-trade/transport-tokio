//! Receiver-side counters. Atomic so recv workers and observability threads
//! can share the handle without a mutex.
//!
//! `kernel_drops` mirrors the `SO_RXQ_OVFL` ancillary counter reported by the
//! kernel each recv; it is monotonic per-socket lifetime.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct ReceiverStats {
    kernel_drops: AtomicU64,
    packets_recv: AtomicU64,
    bytes_recv: AtomicU64,
}

impl ReceiverStats {
    pub fn record_packet(&self, bytes: usize) {
        self.packets_recv.fetch_add(1, Ordering::Relaxed);
        self.bytes_recv.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// Monotonic set: only advance if `drops` exceeds the current value.
    /// `SO_RXQ_OVFL` reports a cumulative counter, but per-message ancillary
    /// data on Linux carries the value at recv time.
    pub fn advance_kernel_drops(&self, drops: u64) {
        let mut cur = self.kernel_drops.load(Ordering::Relaxed);
        while drops > cur {
            match self.kernel_drops.compare_exchange_weak(
                cur,
                drops,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn snapshot(&self) -> ReceiverStatsSnapshot {
        ReceiverStatsSnapshot {
            kernel_drops: self.kernel_drops.load(Ordering::Relaxed),
            packets_recv: self.packets_recv.load(Ordering::Relaxed),
            bytes_recv: self.bytes_recv.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiverStatsSnapshot {
    pub kernel_drops: u64,
    pub packets_recv: u64,
    pub bytes_recv: u64,
}
