//! Tokio-based Transport backend.
//!
//! Currently exposes the [`SharedVecPool`] buffer-pool primitive. UDP and TCP
//! transports arrive in follow-up tasks.

pub mod pool;

pub use pool::{SharedVecPool, VecPool, VecSlab};
