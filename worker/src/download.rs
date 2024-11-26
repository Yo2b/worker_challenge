//! This module provides required downloading features.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use futures::future::{self, FutureExt, TryFutureExt};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use thiserror::Error;
use tokio::{fs, io, sync::Semaphore};

// pub use bytes::Bytes;
pub use reqwest::{Client, Error as ReqwestError, IntoUrl};
// pub use url::Url;

static MAX_HTTP_REQUESTS: Semaphore = Semaphore::const_new(50);
static MAX_FILE_HANDLES: Semaphore = Semaphore::const_new(100);

const CONCURRENT_DOWNLOADS: u8 = 10;

#[derive(Debug, Error)]
/// Errors that can be returned by a download [`Manager`].
pub enum Error {
    #[error(transparent)]
    Reqwest(#[from] ReqwestError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// An HTTP-based download manager to deal with downloading files asynchronously.
///
/// It can handle concurrent downloads:
/// - either on the same pool task using `download::Manager::batch_download()` method,
/// - or on a dedicated pool using `download::Manager::pool_download()` method.
#[derive(Debug)]
pub struct Manager {
    /// The client for HTTP(S) requests.
    client: Client,
    /// The path where files are downloaded.
    path: PathBuf,
}

impl Manager {
    /// Create a new `Manager` with a default client.
    pub fn new(path: PathBuf) -> Self {
        Self {
            client: Client::default(),
            path,
        }
    }

    /// Build a new `Manager` with a specific [`Client`] configuration.
    #[inline]
    pub fn with_client(self, client: Client) -> Self {
        Self { client, ..self }
    }

    #[inline]
    /// Return the root path where files are downloaded.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Download a single file.
    ///
    /// # Errors
    /// This method fails if errors occur with network request or file system operation.
    pub async fn download(&self, path: impl AsRef<Path>, url: impl IntoUrl) -> Result<(), Error> {
        use io::AsyncWriteExt;

        let _ = futures::try_join!(MAX_HTTP_REQUESTS.acquire(), MAX_FILE_HANDLES.acquire()).unwrap();

        let file = fs::File::options()
            .write(true)
            .create_new(true)
            .open(self.path.join(path.as_ref()))
            .await?;

        let mut writer = io::BufWriter::new(file);

        let mut resp = self.client.get(url).send().await?;

        while let Some(chunk) = resp.chunk().await? {
            writer.write_all(&chunk).await?;
        }

        writer.shutdown().await?;
        writer.into_inner().sync_all().await?;

        Ok(())
    }

    /// Download a batch of files concurrently.
    ///
    /// Downloads are executed interlaced on the very same executor than this returned future.
    ///
    /// # Errors
    /// This method fails if errors occur with network requests or file system operations.
    /// Concurrent downloads stop as soon as an error is raised.
    pub async fn batch_download(&self, files: HashMap<impl AsRef<Path>, impl IntoUrl>) -> Result<(), Error> {
        stream::iter(files)
            .map(|(file_name, url)| self.download(file_name, url))
            .buffer_unordered(CONCURRENT_DOWNLOADS as usize)
            .try_collect()
            .await
    }

    /// Download a batch of files concurrently, using a pool in the background.
    ///
    /// Downloads are executed in the background on a dedicated pool and likely multiple executors.
    ///
    /// # Errors
    /// This method fails if errors occur with network requests or file system operations.
    /// Concurrent downloads stop as soon as an error is raised.
    pub async fn pool_download(&self, files: HashMap<impl AsRef<Path>, impl IntoUrl>) -> Result<(), Error> {
        let mut pool = crate::task::Pool::default();
        pool.start(
            CONCURRENT_DOWNLOADS.try_into().unwrap(),
            u16::min(100, files.len() as _).try_into().unwrap(),
        );

        let mut handles = Vec::with_capacity(files.len());

        for (file_name, url) in files {
            let _ = futures::try_join!(MAX_HTTP_REQUESTS.acquire(), MAX_FILE_HANDLES.acquire()).unwrap();

            let path = self.path.join(file_name);
            let file = fs::File::options().write(true).create_new(true).open(path).await?;
            let mut writer = io::BufWriter::new(file);
            let mut stream = self.stream(url.into_url()?);

            let (remote_handle, _) = pool.execute(async move {
                use io::AsyncWriteExt;

                while let Some(chunk) = stream.try_next().await? {
                    writer.write_all(&chunk).await?;
                }

                writer.shutdown().await?;
                writer.into_inner().sync_all().await?;

                Ok::<_, Error>(())
            });

            handles.push(remote_handle.map(Result::unwrap)); // no way the task get aborted
        }

        future::try_join_all(handles).await?;

        // pool.close().await;

        Ok(())
    }

    /// Stream a single file.
    fn stream(&self, url: url::Url) -> impl Stream<Item = Result<bytes::Bytes, Error>> {
        self.client
            .get(url)
            .send()
            .map_ok(|resp| resp.bytes_stream())
            .try_flatten_stream()
            .err_into()
    }
}
