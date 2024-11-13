use std::ops::Deref;
use std::sync::{Arc, RwLock};

use super::*;

// > Each chunk has approximate size of 200 MB and each worker can store up to 1 TB of data on disk.
// This could be mitigated to reserve capacity for less entries by default and allow more frequent dynamic reallocation.
#[allow(clippy::identity_op)]
const DEFAULT_CAPACITY: usize = 1 * 1_024 * 1_024 / 200;

/// Worker's data chunk state.
#[derive(Debug)]
enum DataChunkState {
    Downloading(Box<DataChunk>),
    Ready(Arc<DataChunk>),
}

/// Worker's data manager.
#[derive(Debug)]
pub struct WorkerDataManager {
    data_dir: PathBuf,
    data_chunks: RwLock<HashMap<ChunkId, DataChunkState>>,
}

impl DataManager for WorkerDataManager {
    fn new(data_dir: PathBuf) -> Self {
        let mut manager = Self::new_uninit(data_dir);

        if manager.data_dir.try_exists().expect("root dir cannot be accessed") {
            let data_chunks = manager.data_chunks.get_mut().unwrap();

            tracing::debug!("Populate data chunks from local storage: `{}`", manager.data_dir.display());

            let chunks = Self::walk_dir(&manager.data_dir).expect("chunks cannot be accessed");

            let chunks = chunks
                .into_iter()
                .map(|(_, chunk)| (chunk.id, DataChunkState::Ready(Arc::new(chunk))));

            data_chunks.extend(chunks);
        }

        manager
    }

    fn download_chunk(&self, chunk: DataChunk) {
        let data_chunks = self.data_chunks.read().unwrap();

        if !data_chunks.contains_key(&chunk.id) {
            drop(data_chunks);

            // check if the entry is still vacant, otherwise a download has already been initiated in the meantime
            self.data_chunks.write().unwrap().entry(chunk.id).or_insert_with(|| {
                todo!("download {chunk:?}");

                DataChunkState::Downloading(chunk.into())
            });
        }
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        self.data_chunks.read().unwrap().keys().cloned().collect()
    }

    #[allow(refining_impl_trait_reachable)]
    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<impl DataChunkRef + Deref<Target = DataChunk>> {
        self.data_chunks.read().unwrap().values().find_map(|state| match state {
            DataChunkState::Ready(chunk) if chunk.dataset_id == dataset_id && chunk.block_range.contains(&block_number) => {
                Some(WorkerDataChunkRef(self.chunk_path(chunk), Arc::clone(chunk)))
            }
            _ => None,
        })
    }

    fn delete_chunk(&self, chunk_id: ChunkId) {
        let data_chunks = self.data_chunks.read().unwrap();

        if data_chunks.contains_key(&chunk_id) {
            drop(data_chunks);

            let opt_state = self.data_chunks.write().unwrap().remove(&chunk_id);

            match opt_state {
                Some(DataChunkState::Downloading(chunk)) => todo!("abort {chunk:?}"),
                Some(DataChunkState::Ready(chunk)) => todo!("delete {chunk:?}"),
                None => { /* entry has already been removed in the meantime */ }
            }
        }
    }
}

impl WorkerDataManager {
    fn new_uninit(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            data_chunks: HashMap::with_capacity(DEFAULT_CAPACITY).into(),
        }
    }

    fn chunk_path(&self, chunk: &DataChunk) -> PathBuf {
        let chunk_id = [
            utils::encode_id(&chunk.id),
            utils::encode_id(&chunk.dataset_id),
            chunk.block_range.start.to_string().as_ref(),
            chunk.block_range.end.to_string().as_ref(),
        ]
        .join(std::ffi::OsStr::new("_"));

        self.data_dir.join(chunk_id)
    }

    fn data_chunk(chunk_id: &std::ffi::OsStr) -> std::io::Result<DataChunk> {
        let chunk_id = chunk_id.to_str().ok_or(std::io::ErrorKind::InvalidInput)?;
        match chunk_id.splitn(4, "_").collect::<Vec<_>>()[..] {
            [id, dataset_id, start, end] => Ok(DataChunk {
                id: utils::decode_id(id.as_ref()),
                dataset_id: utils::decode_id(dataset_id.as_ref()),
                block_range: start.parse().map_err(|_| std::io::ErrorKind::InvalidInput)?
                    ..end.parse().map_err(|_| std::io::ErrorKind::InvalidInput)?,
                files: Default::default(),
            }),
            _ => Err(std::io::ErrorKind::InvalidInput)?,
        }
    }

    fn walk_dir(data_dir: &Path) -> std::io::Result<Vec<(PathBuf, DataChunk)>> {
        if !data_dir.is_dir() {
            return Err(std::io::ErrorKind::InvalidInput)?;
        }

        let mut chunks = vec![];

        // assume chunk files are flat stored in directories by unique chunk id / dataset id / block start / block end
        for entry in data_dir.read_dir()? {
            let entry = entry?;

            if entry.file_type()?.is_dir() {
                let chunk_id = entry.file_name();
                let chunk_dir = entry.path();

                tracing::trace!("Read data chunk {chunk_id:?} from local storage: `{}`", chunk_dir.display());

                let chunk_files = vec![];

                // TODO: read chunk descriptor (file names with their associated HTTP URL) from local storage
                // TODO: ultimately check chunk completeness and integrity of stored files

                let chunk = Self::data_chunk(&chunk_id)?.with_files(chunk_files);

                tracing::trace!("Found data chunk {chunk:?} from local storage: `{}`", chunk_dir.display());

                chunks.push((chunk_dir, chunk));
            }
        }

        Ok(chunks)
    }
}

/// Worker's data chunk reference.
#[derive(Debug, Clone)]
pub struct WorkerDataChunkRef(PathBuf, Arc<DataChunk>);

impl Deref for WorkerDataChunkRef {
    type Target = DataChunk;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl DataChunkRef for WorkerDataChunkRef {
    #[inline]
    fn path(&self) -> &Path {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "not an actual test"]
    fn test_size_of() {
        fn print_size_of<T>() {
            println!("{}: {}", std::any::type_name::<T>(), size_of::<T>());
        }

        print_size_of::<WorkerDataManager>();
        print_size_of::<DataChunk>();
        print_size_of::<Arc<DataChunk>>();
        print_size_of::<ChunkId>();

        println!(
            "Default reserved min. size: {} bytes",
            DEFAULT_CAPACITY * (size_of::<ChunkId>() + size_of::<Arc<DataChunk>>())
        );
    }

    const CHUNK_ID: &ChunkId = b"00000000000000000000000000000123";
    const DATASET_ID: &DatasetId = b"cdefghijklmnopqrstuvwxyz01234567";
    const BLOCK_RANGE: Range<u64> = 3..7;
    const BLOCK_IN: u64 = (BLOCK_RANGE.end + BLOCK_RANGE.start) / 2;
    const BLOCK_OUT: u64 = BLOCK_RANGE.end + 1;

    fn expected_data_chunk() -> DataChunk {
        DataChunk::new(*CHUNK_ID, *DATASET_ID, BLOCK_RANGE)
    }

    fn expected_data_chunk_str() -> String {
        format!(
            "{}_{}_{}_{}",
            std::str::from_utf8(CHUNK_ID).unwrap(),
            std::str::from_utf8(DATASET_ID).unwrap(),
            BLOCK_RANGE.start,
            BLOCK_RANGE.end,
        )
    }

    #[test]
    fn test_chunk_path() {
        let chunk = expected_data_chunk();

        let manager = WorkerDataManager::new_uninit(PathBuf::from("a/b"));
        let chunk_path = manager.chunk_path(&chunk);

        assert!(
            chunk_path.iter().eq(["a", "b", &expected_data_chunk_str()]),
            "'{}' didn't match expected path",
            chunk_path.display()
        );
    }

    #[test]
    fn test_data_chunk_from_os_str() {
        let chunk_str = expected_data_chunk_str();

        let chunk = WorkerDataManager::data_chunk(chunk_str.as_ref()).unwrap();

        assert_eq!(chunk, expected_data_chunk());
    }

    #[test]
    #[should_panic]
    fn test_data_manager() {
        let manager = WorkerDataManager::new_uninit(PathBuf::from("path/is/unlikely/to/exist"));

        assert!(manager.list_chunks().is_empty());

        manager.download_chunk(expected_data_chunk());

        assert!(manager.list_chunks().contains(CHUNK_ID));

        manager.delete_chunk(*CHUNK_ID);

        assert!(!manager.list_chunks().contains(CHUNK_ID));

        manager.download_chunk(expected_data_chunk());

        assert!(manager.list_chunks().contains(CHUNK_ID));

        assert_eq!(manager.find_chunk(*DATASET_ID, BLOCK_IN).as_deref(), None);
        assert_eq!(manager.find_chunk(*DATASET_ID, BLOCK_OUT).as_deref(), None);

        {
            let mut data_chunks = manager.data_chunks.write().unwrap();

            println!("{:?}", data_chunks.get(CHUNK_ID));

            data_chunks
                .entry(*CHUNK_ID)
                .and_modify(|state| *state = DataChunkState::Ready(expected_data_chunk().into()));

            println!("{:?}", data_chunks.get(CHUNK_ID));
        }

        let expected = expected_data_chunk();
        assert_eq!(manager.find_chunk(*DATASET_ID, BLOCK_IN).as_deref(), Some(expected).as_ref());
        assert_eq!(manager.find_chunk(*DATASET_ID, BLOCK_OUT).as_deref(), None);

        manager.delete_chunk(*CHUNK_ID);

        assert!(!manager.list_chunks().contains(CHUNK_ID));

        assert_eq!(manager.find_chunk(*DATASET_ID, BLOCK_IN).as_deref(), None);
        assert_eq!(manager.find_chunk(*DATASET_ID, BLOCK_OUT).as_deref(), None);
    }
}
