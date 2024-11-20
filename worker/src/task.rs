//! This module provides required task pooling features.

use std::num::NonZeroU8;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::{self, Future, FutureExt};
use tokio::sync::{mpsc, Mutex};

pub use future::{AbortHandle, Aborted, RemoteHandle};

type Task = future::BoxFuture<'static, ()>;

/// A scalable, generic and widely reusable task pool to deal with asynchronous tasks in the background.
///
/// # Forget tasks
/// Tasks can just be pushed and forgotten using [`Pool::forget()`] method (tasks must return `()`).
///
/// # Keep control over tasks
/// Tasks can alternatively be pushed while still keeping both a remote handle on a task result on completion
/// and an abortable handle to cancel a task using [`Pool::execute()`] method (tasks can return any `T: Send`).
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use worker::task::Pool;
/// let mut pool = Pool::default();
/// pool.start(3.try_into().unwrap());
///
/// for i in 0..=5 {
///     pool.forget(async move { println!("Hello from task #{i}!") });
/// }
///
/// pool.close().await;
/// # })
/// ```
/// ```text
/// Hello from task #0!
/// Hello from task #1!
/// Hello from task #2!
/// Hello from task #3!
/// Hello from task #4!
/// Hello from task #5!
/// ```
#[derive(Debug, Default)]
pub struct Pool {
    /// The pool of workers.
    workers: Vec<Worker>,
    /// The sending part of a channel to push tasks to the workers.
    ///
    /// The next available worker will wait for a task to be received through the channel.
    sender: Option<mpsc::UnboundedSender<Task>>,
}

impl Pool {
    /// Start a pool with `size` workers.
    ///
    /// Once started, tasks can be pushed and will be processed in the background in order.
    ///
    /// _Note: if a task is sent to the pool while not started, it is just lost without executing anything._
    ///
    /// # Panics
    /// This method panics if the pool is already running, ie. when called more than once without stopping the pool in between.
    pub fn start(&mut self, size: NonZeroU8) {
        assert!(self.sender.is_none() && self.workers.is_empty());

        let (sender, receiver) = mpsc::unbounded_channel();

        let receiver = Arc::new(Mutex::new(receiver));

        static WORKER_ID: AtomicUsize = AtomicUsize::new(0);
        let size = size.get().into();
        let id = WORKER_ID.fetch_add(size, Ordering::Relaxed);

        self.workers = (0..size).map(|i| Worker::new(id.wrapping_add(i), Arc::clone(&receiver))).collect();
        self.sender = Some(sender);
    }

    /// Send a task and keep remote and abort handles on it.
    ///
    /// Tasks returning any `T: Send` can be sent while still keeping both a remote handle on a task result on completion
    /// and an abortable handle to cancel a task.
    ///
    /// _Note: if the pool has not been started yet, the task is just lost without executing anything._
    ///
    /// To retrieve a task result on completion, just await on the remote handle:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use worker::task::Pool;
    /// # let mut pool = Pool::default();
    /// # pool.start(3.try_into().unwrap());
    /// let (remote_handle, _) = pool.execute(async { "Hello world!" });
    ///
    /// if let Ok(msg) = remote_handle.await {
    ///     println!("{msg}");
    /// }
    /// # })
    /// ```
    /// ```text
    /// Hello world!
    /// ```
    ///
    /// The remote part can still be released, letting the task complete in the background as simply as:
    /// ```
    /// # use worker::task::Pool;
    /// # let pool = Pool::default();
    /// let (remote_handle, _) = pool.execute(async { "Hello world!" });
    ///
    /// remote_handle.forget()
    /// ```
    /// **Be aware that if you just drop the remote handle, it will automatically cancel the task.**
    ///
    /// To cancel a task, just call `abort_handle.abort()` on the abortable handle.
    /// The remote handle part will then return an `Err(Aborted)` on its side:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use worker::task::{Aborted, Pool};
    /// # let mut pool = Pool::default();
    /// # pool.start(3.try_into().unwrap());
    /// let (remote_handle, abort_handle) = pool.execute(async { "Hello world!" });
    ///
    /// abort_handle.abort();
    ///
    /// assert_eq!(remote_handle.await, Err(Aborted));
    /// # })
    /// ```
    pub fn execute<T: Send>(&self, future: impl Future<Output = T> + Send + 'static) -> (RemoteHandle<Result<T, Aborted>>, AbortHandle) {
        let (abortable, abort_handle) = future::abortable(future);
        let (remote, remote_handle) = abortable.remote_handle();

        self.forget(remote);

        (remote_handle, abort_handle)
    }

    /// Send a task and forget it.
    ///
    /// Tasks returning `()` can be sent and forgotten.
    ///
    /// _Note: if the pool has not been started yet, the task is just lost without executing anything._
    ///
    /// To forget a task:
    /// ```
    /// # use worker::task::Pool;
    /// # let pool = Pool::default();
    /// pool.forget(async { println!("Hello world!") });
    /// ```
    #[inline]
    pub fn forget(&self, future: impl Future<Output = ()> + Send + 'static) {
        if let Some(ref sender) = self.sender {
            sender.send(future.boxed()).unwrap();
        }
    }

    /// Stop current pool of workers, waiting for all pending tasks to complete.
    ///
    /// Once stopped, the pool can be started again with any number of workers.
    ///
    /// _Note: if a task is sent to the pool while stopped, it is just lost without executing anything._
    pub async fn stop(&mut self) {
        let _ = self.sender.take().as_ref().map(mpsc::UnboundedSender::downgrade);
        let workers = std::mem::take(&mut self.workers);

        future::join_all(workers.into_iter().inspect(|worker| {
            tracing::debug!("Stopping worker {}...", worker.id);
        }))
        .await;
    }

    /// Close current pool of workers, waiting for all pending tasks to complete.
    ///
    /// Once closed, the pool cannot be used since it is consumed.
    pub async fn close(mut self) {
        self.stop().await
    }
}

#[derive(Debug)]
struct Worker {
    id: usize,
    handle: tokio::task::JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::UnboundedReceiver<Task>>>) -> Worker {
        tracing::debug!("Starting worker {id}...");

        Worker {
            id,
            handle: tokio::spawn(async move {
                loop {
                    let message = receiver.lock().await.recv().await;

                    match message {
                        Some(task) => {
                            tracing::debug!("Executing task on worker {id}...");

                            task.await;
                        }
                        None => {
                            tracing::debug!("All tasks exhausted, shutting down worker {id}.");
                            break;
                        }
                    }
                }
            }),
        }
    }
}

impl Future for Worker {
    type Output = Result<(), tokio::task::JoinError>;

    #[inline]
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.handle.poll_unpin(cx)
    }
}
