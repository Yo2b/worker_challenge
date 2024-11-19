//! A simple crate providing worker features.
//!
//! A generic, robust and efficient crate providing features dedicated to:
//! - handling (blockchain) data chunks,
//! - managing generic asynchronous tasks synchronously,
//! - downloading files,
//! - storing data chunks.
//!
//! It obviously makes use of both synchronous and asynchronous implementations of synchronization primitives such as `atomic` primitive types,
//! favourably using `RwLock`s over `Mutex`s wherever possible, and also `Semaphore`s to tweak and limit consumed system resources, as well as
//! _multi-producer single-consumer_ or _multi-producer multiple-consumer_ patterns using `mpsc` channels or notifications for shared-memory
//! communication between threads/tasks.
//!
//! As a **strong hypothesis**, we can assume that:
//! - `chunk_id` generation is **out-of-scope**;
//! - `(chunk_id, dataset_id, block_range)` tuples are made **unique**.
//!
//! Based on this assumption, data chunk entries are cached within an in-memory hashmap, with its `chunk_id` as the key and a wrapped `DataChunk`
//! as the value. Depending on its internal state, the `DataChunk` part is wrapped in a `DataChunkState` enum variant, together with some related
//! additional context. To preserve a minimal memory footprint for the hashmap, those internal states may be `Box`'ed or `Arc`'ed depending on they
//! intend to be shared or not. Moreover, note that a `chunk_id` could be returned and/or successfully considered when listing or deleting chunks in
//! a data manager, but an actual `DataChunkRef` will be successfully returned only when the `DataChunk` is fully available from the local storage.
//!
//! Here is the naive representation of a flat local data chunk storage in its very first version:
//!
//! ```text
//!     - root local storage
//!       L {chunk_id}_{dataset_id}_{block_start}_{block_end}
//!         L chunk.nfo
//!         L data chunk file 1
//!         L data chunk file 2
//!         L data chunk file ...
//!       L {chunk_id}_{dataset_id}_{block_start}_{block_end}.tmp
//!         L partial data chunk file 1
//!         L partial data chunk file ...
//! ```
//!
//! This representation has been kept very stupid simple to efficiently move on concurrency and asynchronous parts of the challenge. There is no
//! doubt that it is not the best representation for a fast/full access to a whole dataset's chunks. But it is not the trickier part to adapt.

mod data;
pub use data::*;

pub mod download;
pub mod task;
