# worker\_challenge

[Documentation](https://yo2b.github.io/worker_challenge)

## Wording of the challenge
> The data is divided into chunks. Each chunk corresponds to particular blockchain and block range, and stores block related data.
> 
> There is a set of workers. Each worker downloads an assigned subset of data chunks from a large persistent storage and is responsible for serving queries to assigned data.
> 
> Your task is to develop the data management component of a worker...

## Approach
Practicing async & concurrent Rust intensively is a strong prerequisite for this challenge.

With the big picture in mind, I decided to move forward step-by-step in the design of a generic, robust and efficient crate, adding _adhoc_ features dedicated to:
- handling data chunks,
- managing asynchronous tasks synchronously,
- downloading files,
- storing data chunks.

It obviously makes use of both synchronous and asynchronous implementations of synchronization primitives such as `atomic` primitive types, favourably using `RwLock`s over `Mutex`s wherever possible, and also `Semaphore`s to tweak and limit consumed system resources, as well as _multi-producer single-consumer_ or _multi-producer multiple-consumer_ patterns using `mpsc` channels or notifications for shared-memory communication between threads/tasks.

As a **strong hypothesis** prior to this challenge, I assumed that `chunk_id` generation is **out-of-scope** and `(chunk_id, dataset_id, block_range)` tuples are made **unique**.

Based on this assumption, data chunk entries are cached within an in-memory hashmap, with its `chunk_id` as the key and a wrapped `DataChunk` as the value. Depending on its internal state, the `DataChunk` part is wrapped in a `DataChunkState` enum variant, together with some related additional context. To preserve a minimal memory footprint for the hashmap, those internal states may be `Box`'ed or `Arc`'ed depending on they intend to be shared or not. Moreover, note that a `chunk_id` could be returned and/or successfully considered when listing or deleting chunks in a data manager, but an actual `DataChunkRef` will be successfully returned only when the `DataChunk` is fully available from the local storage.

Here is the naive representation of a flat local data chunk storage in its very first version:

    - root local storage
      L {chunk_id}_{dataset_id}_{block_start}_{block_end}
        L chunk.nfo
        L data chunk file 1
        L data chunk file 2
        L data chunk file ...
      L {chunk_id}_{dataset_id}_{block_start}_{block_end}.tmp
        L partial data chunk file 1
        L partial data chunk file ...

This representation has been kept very stupid simple to efficiently move on concurrency and asynchronous parts of the challenge. There is no doubt that it is not the best representation for a fast/full access to a whole dataset's chunks. But it is not the trickier part to adapt.

I also wanted to be careful about documenting and testing, as I would with any standard project.

## Workspace & projects
This workspace is made up of the following projects:
* `worker`: a `lib` crate dedicated to handling data chunks and spawning tasks with minimal overhead and blazing-fast performances.
* [TODO] `mock`: a `bin` crate dedicated to mock up a worker managing a set of data chunk files, as a proof-of-concept of the `worker` crate.

## Features
The `worker` crate aims to provide an efficient shared library with a minimal set of features to handle data chunks in a local storage and spawn download tasks.

It is mainly composed of:
- A `data::WorkerDataManager` task orchestrator managing existing data chunks, downloading new ones and storing them to a local storage. It strictly implements the imposed `DataManager` trait, handles `DataChunk` structs and returns `DataChunkRef` trait implementors.
- A scalable, generic and widely reusable `task::Pool` to deal with asynchronous tasks in the background:
  - Tasks can just be pushed and forgotten using `task::Pool::forget()` method (tasks must return `()`).
  - Tasks can be pushed while still keeping both a remote handle on a task result on completion and an abortable handle to cancel a task using `task::Pool::execute()` method (tasks can return any `T: Send`).
- An asynchronous HTTP-based `download::Manager` to deal with downloading data chunk files. It can handle concurrent downloads:
  - either on the same pool task using `download::Manager::batch_download()` method,
  - or on a dedicated pool using `download::Manager::pool_download()` method.

## Dependencies
The crates in this workspace may rely on other renowned, widely tried and tested crates developed by the ever-growing Rust community, amongst others:

* [``tokio``](https://crates.io/crates/tokio) crate for event-driven, async I/O capabilities.
* [``futures``](https://crates.io/crates/futures) crate for futures/streams/sinks adaptations.
* [``reqwest``](https://crates.io/crates/reqwest) crate for asynchronous HTTP(S)-based client capabilities.
* [``tracing``](https://crates.io/crates/rocket) crate for logging capabilities.
* [TODO] [``clap``](https://crates.io/crates/clap) crate for CLI management and command-line arguments parsing.
* [TODO] [``serde``](https://crates.io/crates/serde) crate for (de)serialization of data structures.

## Short-comings
- The imposed traits will not be improved nor breaking-changed to strictly stick with the framework of this challenge (eg. some methods should better return a `Result`). Especially, the `DataManager` implementation will be kept synchronous code.
- The `DataManager::new()` trait method implementation will only deal with loading chunk description and rely on stored data, without any additional check for completeness or data integrity.
- Chunk descriptors (file names with their associated HTTP URLs) are not stored to local storage for now.
- The `WorkerDataManager`'s pool is conveniently reused for now to achieve chunk deletion tasks, ~~but it could flood the pool if too many deletions are required while keeping unreleased chunk references~~.
- Error handling is minimalist (ie. no download retries, some IO errors are ignored...).
- _"Think about possibilities of receiving multiple download requests for the same chunk or chunk deletion request followed by immediate download."_:
  - Multiple download requests for the same chunk are considered only once, assuming that `(chunk_id, dataset_id, block_range)` tuples are unique.
  - Chunk deletion request followed by immediate download is considered as a "forced" new download.
  - I suspect ~~there could still be a risk of time-of-check to time-of-use (TOCTOU) bugs (or kind of)~~ when requesting:
    - download chunk
    - delete chunk before completion (abort) -> postpone cleaning task
    - download chunk before execution or completion of cleaning task -> postpone downloading task
      - ~~colliding downloading task~~
      - ~~colliding cleaning task~~

## Improvements
- Add more ~~doc comments and~~ unit tests, as well as full test data sets.
- ~~Properly finalize `task::Pool` when `WorkerDataManager` is dropped~~.
- ~~Fix data chunk deletion after all data chunk references have been dropped~~.
- ~~Use dedicated temp directories for processing chunk downloads, eg. make them unique replacing `tmp` extension with timestamp- and/or random-based naming~~.
- Manage I/O errors, HTTP errors, data chunk errors... and deal with HTTP retries.
- Implement `serde` (de)serialization traits for `DataChunk` type and read/write data chunk descriptors to local storage.
- Check for data integrity when reading/writing to local data storage.
- Use a real cross-platform crate to deal with encoding/decoding of chunk ids and dataset ids instead of `utils::encode_id()` and `utils::decode_id()` convenient helper functions.
- Make use of [`parking_lot`](https://docs.rs/parking_lot/0.12.3/parking_lot/index.html) crate for simpler, safer, fairer usage, eg. avoiding `Option::unwrap()` call in every `RwLock` handling, and more advanced and efficient synchronization primitives, eg. using [`RwLockUpgradableReadGuard`](https://docs.rs/lock_api/0.4.7/lock_api/struct.RwLockUpgradableReadGuard.html) (to be tested).
- Make use of [`dashmap`](https://docs.rs/dashmap/latest/dashmap/index.html) crate for better concurrent access within `WorkerDataManager` (to be tested).

## Further ideas:
- `WorkerDataManager` should be splitted to isolate and better specialize its local storage management part.
- The `WorkerDataManager::new()` method implementation could easily return earlier and load existing chunks in a background task. Still it will have to wait for the write lock access to be released before being used in other threads.
- `download::Manager` could be instantiated once with the `WorkerDataManager` and handle concurrent chunk download either on the same pool task using `download::Manager::batch_download()` or on a dedicated pool using `download::Manager::pool_download()` methods (to be tested).
