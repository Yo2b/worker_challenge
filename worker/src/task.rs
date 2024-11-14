use std::num::NonZeroU8;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::future::{self, Future, FutureExt};
use tokio::sync::{mpsc, Mutex};

type Task = future::BoxFuture<'static, ()>;

#[derive(Debug, Default)]
pub struct Pool {
    workers: Vec<Worker>,
    sender: Option<mpsc::UnboundedSender<Task>>,
}

impl Pool {
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

    pub fn execute<T: Send>(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> (future::RemoteHandle<Result<T, future::Aborted>>, future::AbortHandle) {
        let (abortable, abort_handle) = future::abortable(future);
        let (remote, remote_handle) = abortable.remote_handle();

        self.forget(remote);

        (remote_handle, abort_handle)
    }

    #[inline]
    pub fn forget(&self, future: impl Future<Output = ()> + Send + 'static) {
        if let Some(ref sender) = self.sender {
            sender.send(future.boxed()).unwrap();
        }
    }

    pub async fn stop(&mut self) {
        let _ = self.sender.take().as_ref().map(mpsc::UnboundedSender::downgrade);
        let workers = std::mem::take(&mut self.workers);

        future::join_all(workers.into_iter().map(|worker| {
            tracing::debug!("Stopping worker {}...", worker.id);

            worker.handle
        }))
        .await;
    }

    pub async fn close(mut self) {
        self.stop().await
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        tracing::debug!("Dropping pool...");

        // futures::executor::block_on(self.stop())
        // tokio::task::block_in_place(|| {
        //     tokio::runtime::Handle::current().block_on(self.stop())
        // })
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

// impl Future for Worker {
//     type Output = Result<(), tokio::task::JoinError>;

//     #[inline]
//     fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
//         self.handle.poll_unpin(cx)
//     }
// }
