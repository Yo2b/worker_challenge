use std::num::NonZeroU8;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};

use futures::future::AbortHandle;
use tokio::sync::{mpsc, Notify};

use super::*;

type DeletionSender = mpsc::UnboundedSender<PathBuf>;

// > Each chunk has approximate size of 200 MB and each worker can store up to 1 TB of data on disk.
// This could be mitigated to reserve capacity for less entries by default and allow more frequent dynamic reallocation.
#[allow(clippy::identity_op)]
const DEFAULT_CAPACITY: usize = 1 * 1_024 * 1_024 / 200;
const POOL_SIZE: NonZeroU8 = unsafe { NonZeroU8::new_unchecked(10) };
const TEMP_EXT: &str = "tmp";

/// Worker's data chunk state.
#[derive(Debug)]
enum DataChunkState {
    /// Downloading state.
    ///
    /// When requesting for a data chunk download, this entry state is added to the in-memory [`HashMap`] cache for the associated `chunk.id` key.
    /// The state context is made of the `DataChunk` description and an `AbortHandle` on the task in the pool.
    Downloading(Option<Box<(DataChunk, AbortHandle)>>),
    /// Ready state.
    ///
    /// When completing a data chunk download or reading a data chunk from the local storage, this entry state is added or replaced in-place in the
    /// in-memory [`HashMap`] cache for the associated `chunk.id` key. The state context is made of the originally owned `DataChunk` wrapped in the
    /// first `WorkerDataChunkRef`.
    Ready(WorkerDataChunkRef),
}

/// Worker's data manager.
///
/// A task orchestrator managing existing data chunks, downloading new ones and storing them to a local storage.
/// It implements the [`DataManager`] trait, handles [`DataChunk`] structs and returns [`DataChunkRef`] trait implementors.
#[derive(Debug)]
pub struct WorkerDataManager {
    /// The path to store the data in the local storage.
    data_dir: PathBuf,
    /// An in-memory `HashMap` cache protected for concurrent R/W access for managed `chunk.id` keys and their associated `DataChunk` description.
    data_chunks: Arc<RwLock<HashMap<ChunkId, DataChunkState>>>,
    // /// A manager to download files in the background.
    // download_manager: crate::download::Manager,
    /// A pool to manage tasks in the background.
    task_pool: crate::task::Pool,
    /// A data channel to send data chunk paths in the local storage for background deletion
    deletion_sender: Option<DeletionSender>,
    /// A control channel to notify the deletion channel to stop accepting data chunk paths
    deletion_notify: Arc<Notify>,
}

impl DataManager for WorkerDataManager {
    /// Create a fully "initialized" `WorkerDataManager` instance.
    ///
    /// It is "initialized" in the way that its in-memory [`HashMap`] cache is populated with data chunks existing in the local storage
    /// and its task [`Pool`](crate::task::Pool) has been started to accept pending tasks.
    fn new(data_dir: PathBuf) -> Self {
        let mut manager = Self::new_uninit(data_dir);

        if manager.data_dir.try_exists().expect("root dir cannot be accessed") {
            let data_chunks = Arc::get_mut(&mut manager.data_chunks).unwrap();
            let data_chunks = data_chunks.get_mut().unwrap();

            tracing::debug!("Populate data chunks from local storage: `{}`", manager.data_dir.display());

            let chunks = Self::walk_dir(&manager.data_dir).expect("chunks cannot be accessed");

            let chunks = chunks
                .into_iter()
                .map(|(path, chunk)| (chunk.id, DataChunkState::Ready(WorkerDataChunkRef::new(path, chunk))));

            data_chunks.extend(chunks);
        }

        tracing::debug!("Initiate task pool.");

        manager.task_pool.start(POOL_SIZE);

        tracing::debug!("Initiate deletion channel.");

        let notify = Arc::clone(&manager.deletion_notify);
        let (sender, mut receiver) = mpsc::unbounded_channel();
        manager.deletion_sender.replace(sender);

        // conveniently reuse task pool to achieve deletion tasks
        manager.task_pool.forget(async move {
            tracing::debug!("Deletion channel started.");

            loop {
                tokio::select! {
                    // biased;

                    _ = notify.notified(), if !receiver.is_closed() => {
                        receiver.close();
                        tracing::trace!("Deletion channel closed.");
                    }

                    Some(path) = receiver.recv() => {
                        tracing::debug!("Deleting data chunk from local storage: `{}`", path.display());

                        // delete chunk
                        let _ = tokio::fs::remove_dir_all(path).await;
                    }

                    else => {
                        break;
                    }
                }
            }

            tracing::debug!("Deletion channel exhausted, shutting down.");
        });

        manager
    }

    /// Schedule `chunk` download in background.
    fn download_chunk(&self, chunk: DataChunk) {
        if self.data_chunks.read().unwrap().contains_key(&chunk.id) {
            return;
        }

        self.data_chunks
            .write()
            .unwrap()
            .entry(chunk.id)
            .and_modify(|_state| {
                // the entry is occupied, meaning its state is already downloading or ready
            })
            .or_insert_with(|| {
                // the entry is still vacant, meaning no download has been initiated in the meantime
                let data_chunks = Arc::clone(&self.data_chunks);

                let path = self.chunk_path(&chunk);
                let files = chunk.files.clone();

                let (remote_handle, abort_handle) = self.task_pool.execute(async move {
                    tracing::debug!("Downloading data chunk to local storage: `{}`", path.display());

                    // let path = path.canonicalize()?;
                    let tmp = path.with_extension(TEMP_EXT);

                    tokio::fs::create_dir_all(&tmp).await?;

                    // TODO: write chunk descriptor (file names with their associated HTTP URL) to local storage

                    let manager = crate::download::Manager::new(tmp);
                    manager.batch_download(files).await?;
                    // manager.pool_download(files).await?;

                    tokio::fs::rename(manager.path(), &path).await?;

                    tracing::debug!("Downloaded data chunk to local storage: `{}`", path.display());

                    // chunk can still be canceled while awaiting for a write access,
                    // in which case entry has been removed otherwise it can be granted to ready state
                    let canceled = data_chunks
                        .write()
                        .unwrap()
                        .get_mut(&chunk.id)
                        .map(|state| match state {
                            DataChunkState::Downloading(ctx) => {
                                let (chunk, handle) = *ctx.take().unwrap();

                                debug_assert!(!handle.is_aborted());

                                *state = DataChunkState::Ready(WorkerDataChunkRef::new(path.clone(), chunk));
                            }
                            _ => unreachable!(),
                        })
                        .is_none();

                    if canceled {
                        tracing::debug!("Deleting canceled data chunk from local storage: `{}`", path.display());

                        tokio::fs::remove_dir_all(&path).await?;
                    }

                    Ok::<_, crate::download::Error>(())
                });

                remote_handle.forget(); // forget remote handle for now but should be used to deal with errors

                DataChunkState::Downloading(Some(Box::new((chunk, abort_handle))))
            });
    }

    /// List chunks that are currently available.
    fn list_chunks(&self) -> Vec<ChunkId> {
        self.data_chunks.read().unwrap().keys().copied().collect()
    }

    /// Find a chunk from a given dataset, that is responsible for `block_number`.
    ///
    /// This trait method implementation refines the initial trait constraints on the returned type so that it can be coerced to its underlying
    /// [`DataChunk`] when dereferencing.
    #[allow(refining_impl_trait_reachable)]
    fn find_chunk(&self, dataset_id: DatasetId, block_number: u64) -> Option<impl DataChunkRef + Deref<Target = DataChunk>> {
        self.data_chunks.read().unwrap().values().find_map(|state| match state {
            DataChunkState::Ready(chunk_ref) if chunk_ref.dataset_id == dataset_id && chunk_ref.block_range.contains(&block_number) => {
                Some(chunk_ref.clone())
            }
            _ => None,
        })
    }

    /// Schedule data chunk for deletion in background.
    fn delete_chunk(&self, chunk_id: ChunkId) {
        if !self.data_chunks.read().unwrap().contains_key(&chunk_id) {
            return;
        }

        let opt_state = self.data_chunks.write().unwrap().remove(&chunk_id);

        match opt_state {
            Some(DataChunkState::Downloading(ctx)) => {
                let (chunk, handle) = *ctx.unwrap();

                handle.abort();

                let mut path = self.chunk_path(&chunk);
                path.set_extension(TEMP_EXT);

                tracing::debug!("Aborted data chunk download to local storage: `{}`", path.display());

                // delete incomplete chunk
                if let Some(ref sender) = self.deletion_sender {
                    sender.send(path).unwrap();
                }
            }
            Some(DataChunkState::Ready(chunk_ref)) => {
                tracing::debug!("Removing data chunk from local storage: `{}`", chunk_ref.path().display());

                debug_assert!(self.deletion_sender.is_some());

                // postpone chunk deletion
                chunk_ref.notify(self.deletion_sender.clone());
            }
            None => { /* entry has already been removed in the meantime */ }
        }
    }
}

impl WorkerDataManager {
    /// Create a new "uninitialized" `WorkerDataManager` instance.
    ///
    /// It is "uninitialized" in the way that neither its in-memory [`HashMap`] cache is populated with data chunks existing in the local storage
    /// nor its task [`Pool`](crate::task::Pool) has been started to accept pending tasks.
    fn new_uninit(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            data_chunks: Arc::new(RwLock::new(HashMap::with_capacity(DEFAULT_CAPACITY))),
            // download_manager: crate::download::Manager::default(),
            task_pool: crate::task::Pool::default(),
            deletion_notify: Arc::new(Notify::new()),
            deletion_sender: None,
        }
    }

    /// Return the expected path in the local storage related to this data chunk.
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

    /// Return the expected data chunk related to this path in the local storage.
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

    /// Walk through the local storage and return all valid data chunk stored in it.
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

                if chunk_dir.extension().is_some_and(|ext| ext == TEMP_EXT) {
                    tracing::trace!("Clean incomplete chunk from local storage: `{}`", chunk_dir.display());

                    std::fs::remove_dir_all(chunk_dir)?;
                } else {
                    tracing::trace!("Read data chunk {chunk_id:?} from local storage: `{}`", chunk_dir.display());

                    let chunk_files = vec![];

                    // TODO: read chunk descriptor (file names with their associated HTTP URL) from local storage
                    // TODO: ultimately check chunk completeness and integrity of stored files

                    let chunk = Self::data_chunk(&chunk_id)?.with_files(chunk_files);

                    tracing::trace!("Found data chunk {chunk:?} from local storage: `{}`", chunk_dir.display());

                    chunks.push((chunk_dir, chunk));
                }
            }
        }

        Ok(chunks)
    }

    /// Gracefully close this `WorkerDataManager`.
    ///
    /// It closes the deletion channel and waits for the task pool to complete all its pending tasks.
    pub async fn close(mut self) {
        tracing::trace!("Closing deletion channel...");
        self.deletion_notify.notify_one();
        tokio::task::yield_now().await;

        tracing::trace!("Closing task pool...");
        self.task_pool.stop().await;
        tracing::trace!("Task pool closed.");
    }
}

impl Drop for WorkerDataManager {
    fn drop(&mut self) {
        // futures::executor::block_on(self.pool.stop());
        // // tokio::task::block_in_place(|| {
        // //     tokio::runtime::Handle::current().block_on(self.pool.stop());
        // // })
    }
}

/// Worker's data chunk reference.
///
/// A data chunk remains available and untouched until this reference is dropped.
#[derive(Debug, Clone)]
struct WorkerDataChunkRef {
    ctx: Arc<(PathBuf, DataChunk, Mutex<Option<DeletionSender>>)>,
}

impl WorkerDataChunkRef {
    fn new(path: PathBuf, chunk: DataChunk) -> Self {
        Self {
            ctx: Arc::new((path, chunk, Mutex::new(None))),
        }
    }

    fn notify(&self, sender: Option<DeletionSender>) {
        *self.ctx.2.lock().unwrap() = sender;
    }
}

impl Deref for WorkerDataChunkRef {
    type Target = DataChunk;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.ctx.1
    }
}

impl DataChunkRef for WorkerDataChunkRef {
    #[inline]
    fn path(&self) -> &Path {
        &self.ctx.0
    }
}

impl Drop for WorkerDataChunkRef {
    fn drop(&mut self) {
        if let Some(ctx) = Arc::get_mut(&mut self.ctx) {
            // send a notification whenever this reference is the only remaining one
            if let Some(sender) = ctx.2.get_mut().unwrap().take() {
                let path = std::mem::take(&mut ctx.0);

                tracing::trace!(
                    "Sending data chunk path into deletion channel for local storage: `{}`",
                    path.display()
                );

                sender.send(path).unwrap();
            }
        }
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
        print_size_of::<WorkerDataChunkRef>();
        print_size_of::<DataChunkState>();
        print_size_of::<DataChunk>();
        print_size_of::<PathBuf>();
        print_size_of::<mpsc::UnboundedSender<PathBuf>>();
        print_size_of::<Notify>();
        print_size_of::<AbortHandle>();
        print_size_of::<Option<Box<(DataChunk, AbortHandle)>>>();
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
    #[tracing_test::traced_test]
    fn test_data_manager_uninit() {
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
    }

    #[tokio::test/* (flavor = "multi_thread") */]
    #[tracing_test::traced_test]
    async fn test_data_manager_download() {
        let temp = tempfile::tempdir().unwrap();

        let manager = WorkerDataManager::new(temp.path().to_path_buf());

        manager.download_chunk(expected_data_chunk());

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let expected = expected_data_chunk();
        assert_eq!(manager.find_chunk(*DATASET_ID, BLOCK_IN).as_deref(), Some(expected).as_ref());

        println!("Dropping manager...");
        manager.close().await;
        println!("Manager dropped!");
    }

    #[tokio::test/* (flavor = "multi_thread") */]
    #[tracing_test::traced_test]
    async fn test_data_manager_delete_no_ref() {
        let temp = tempfile::tempdir().unwrap();

        let manager = WorkerDataManager::new(temp.path().to_path_buf());

        manager.download_chunk(expected_data_chunk());

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let chunk = manager.find_chunk(*DATASET_ID, BLOCK_IN).unwrap();
        assert_eq!(*chunk, expected_data_chunk());
        drop(chunk);

        manager.delete_chunk(*CHUNK_ID);

        println!("Dropping manager...");
        manager.close().await;
        println!("Manager dropped!");
    }

    #[tokio::test/* (flavor = "multi_thread") */]
    #[tracing_test::traced_test]
    async fn test_data_manager_delete_one_ref() {
        let temp = tempfile::tempdir().unwrap();

        let manager = WorkerDataManager::new(temp.path().to_path_buf());

        manager.download_chunk(expected_data_chunk());

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let chunk = manager.find_chunk(*DATASET_ID, BLOCK_IN).unwrap();
        assert_eq!(*chunk, expected_data_chunk());
        manager.delete_chunk(*CHUNK_ID);

        tokio::task::yield_now().await;

        println!("Dropping chunk ref...");
        drop(chunk);
        println!("Chunk ref dropped!");

        tokio::task::yield_now().await;

        println!("Dropping manager...");
        manager.close().await;
        println!("Manager dropped!");
    }
}
