use super::*;

/// Worker's data manager.
#[derive(Debug)]
pub struct WorkerDataManager;

impl DataManager for WorkerDataManager {
    fn new(data_dir: PathBuf) -> Self {
        unimplemented!()
    }

    fn download_chunk(&self, chunk: DataChunk) {
        unimplemented!()
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        unimplemented!()
    }

    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<impl DataChunkRef> {
        unimplemented!();

        None::<WorkerDataChunkRef>
    }

    fn delete_chunk(&self, chunk_id: ChunkId) {
        unimplemented!()
    }
}

/// Worker's data chunk reference.
#[derive(Debug, Clone)]
pub struct WorkerDataChunkRef;

impl DataChunkRef for WorkerDataChunkRef {
    fn path(&self) -> &Path {
        unimplemented!()
    }
}
