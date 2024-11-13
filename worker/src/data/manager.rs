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

impl WorkerDataManager {
    fn chunk_path(chunk: &DataChunk) -> PathBuf {
        let chunk_id = [
            utils::encode_id(&chunk.id),
            utils::encode_id(&chunk.dataset_id),
            chunk.block_range.start.to_string().as_ref(),
            chunk.block_range.end.to_string().as_ref(),
        ]
        .join(std::ffi::OsStr::new("_"));

        PathBuf::from(chunk_id)
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

                let chunk_files = vec![];

                // TODO: read chunk descriptor (file names with their associated HTTP URL) from local storage
                // TODO: ultimately check chunk completeness and integrity of stored files

                let chunk = Self::data_chunk(&chunk_id)?.with_files(chunk_files);

                chunks.push((chunk_dir, chunk));
            }
        }

        Ok(chunks)
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

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK_ID: &ChunkId = b"00000000000000000000000000000123";
    const DATASET_ID: &DatasetId = b"cdefghijklmnopqrstuvwxyz01234567";
    const BLOCK_RANGE: Range<u64> = 3..7;

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

        let chunk_path = WorkerDataManager::chunk_path(&chunk);

        assert!(
            chunk_path.iter().eq([expected_data_chunk_str().as_ref() as &std::ffi::OsStr]),
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
}
