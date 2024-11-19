use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};

mod manager;
pub use manager::*;

mod utils;

/// A dataset (blockchain) id.
pub type DatasetId = [u8; 32];
/// A data chunk id.
pub type ChunkId = [u8; 32];

/// Data chunk description.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub struct DataChunk {
    /// Data chunk id.
    id: ChunkId,

    /// Dataset (blockchain) id.
    dataset_id: DatasetId,

    /// Block range this chunk is responsible for (around 100 - 10000 blocks).
    block_range: Range<u64>,

    /// Data chunk files.
    ///
    /// A mapping between file names and HTTP URLs to download files from.
    /// Usually contains 1 - 10 files of various sizes.
    /// The total size of all files in the chunk is about 200 MB.
    files: HashMap<String, String>,
}

impl DataChunk {
    /// Create a new `DataChunk`.
    pub fn new(id: ChunkId, dataset_id: DatasetId, block_range: Range<u64>) -> Self {
        Self {
            id,
            dataset_id,
            block_range,
            files: Default::default(),
        }
    }

    /// Build a new `DataChunk` with given data chunk files.
    #[inline]
    pub fn with_files<I: IntoIterator<Item = (String, String)>>(self, iter: I) -> Self {
        Self {
            files: FromIterator::from_iter(iter),
            ..self
        }
    }
}

/// Data manager interface.
pub trait DataManager: Send + Sync {
    /// Create a new `DataManager` instance, that will use `data_dir` to store the data.
    ///
    /// When `data_dir` is not empty, this method should create a list of fully downloaded chunks
    /// and use it as initial state.
    fn new(data_dir: PathBuf) -> Self;

    /// Schedule `chunk` download in background.
    fn download_chunk(&self, chunk: DataChunk);

    /// List chunks that are currently available.
    fn list_chunks(&self) -> Vec<ChunkId>;

    /// Find a chunk from a given dataset, that is responsible for `block_number`.
    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<impl DataChunkRef>;

    /// Schedule data chunk for deletion in background.
    fn delete_chunk(&self, chunk_id: ChunkId);
}

/// Data chunk reference.
///
/// It must remain available and untouched till this reference is not dropped.
pub trait DataChunkRef: Send + Sync + Clone {
    /// Data chunk directory.
    fn path(&self) -> &Path;
}
