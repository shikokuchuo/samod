use samod_core::PeerId;

use crate::{
    DocumentServed, Repo,
    announce_policy::{AlwaysAnnounce, AnnouncePolicy, LocalAnnouncePolicy},
    runtime::{LocalRuntimeHandle, RuntimeHandle},
    storage::{InMemoryStorage, LocalStorage, Storage},
};

/// How to run concurrent documents
///
/// See the [concurrency section](./index.html#concurrency) of the module level
/// documentation
pub enum ConcurrencyConfig {
    /// Run each document in a separate task on the async runtime
    AsyncRuntime,
    /// Run each document on a rayon threadpool
    #[cfg(feature = "threadpool")]
    Threadpool(rayon::ThreadPool),
}

/// A struct for configuring a [`Repo`](crate::Repo)
///
/// ## `Send` and non-`Send` futures
///
/// Once you've finished configuring the builder, you call either [`load`](Self::load) or
/// [`load_local`](Self::load_local) to create a [`Repo`]. The difference between the two is that
/// [`load`](Self::load) is only available for runtimes which implement `RuntimeHandle` and
/// thus require `Send` futures (such as tokio). [`load_local`](Self::load_local) on the other
/// hand works with runtimes that implement [`LocalRuntimeHandle`] and thus does
/// not require [`Send`] futures. If you want to use
/// [`load_local`](Self::load_local) you need to configure the storage and
/// announce policy implementations to be implementations of [`LocalStorage`]
/// and [`LocalAnnouncePolicy`] respectively. See the [module level
/// documentation on runtimes](./index.html#runtimes) for more details.
pub struct RepoBuilder<S, R, A> {
    pub(crate) storage: S,
    pub(crate) runtime: R,
    pub(crate) announce_policy: A,
    pub(crate) peer_id: Option<PeerId>,
    pub(crate) concurrency: ConcurrencyConfig,
    pub(crate) on_document_served: Option<Box<dyn Fn(DocumentServed) + Send + Sync>>,
}

impl<S, R, A> RepoBuilder<S, R, A> {
    pub fn with_storage<S2>(self, storage: S2) -> RepoBuilder<S2, R, A> {
        RepoBuilder {
            storage,
            peer_id: self.peer_id,
            runtime: self.runtime,
            announce_policy: self.announce_policy,
            concurrency: self.concurrency,
            on_document_served: self.on_document_served,
        }
    }

    pub fn with_runtime<R2>(self, runtime: R2) -> RepoBuilder<S, R2, A> {
        RepoBuilder {
            runtime,
            peer_id: self.peer_id,
            storage: self.storage,
            announce_policy: self.announce_policy,
            concurrency: self.concurrency,
            on_document_served: self.on_document_served,
        }
    }

    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = Some(peer_id);
        self
    }

    pub fn with_announce_policy<A2>(self, announce_policy: A2) -> RepoBuilder<S, R, A2> {
        RepoBuilder {
            runtime: self.runtime,
            peer_id: self.peer_id,
            storage: self.storage,
            announce_policy,
            concurrency: self.concurrency,
            on_document_served: self.on_document_served,
        }
    }

    /// Configure how the repository should process concurrent documents
    ///
    /// See the [concurrency section](./index.html#concurrency) of the module level
    /// documentation
    pub fn with_concurrency(mut self, concurrency: ConcurrencyConfig) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Register a callback that fires when document sync data is first
    /// transmitted to a remote peer for a given (connection, document) pair.
    ///
    /// The callback receives a [`DocumentServed`] value synchronously during
    /// event processing and must not block.
    pub fn with_on_document_served(
        mut self,
        callback: impl Fn(DocumentServed) + Send + Sync + 'static,
    ) -> Self {
        self.on_document_served = Some(Box::new(callback));
        self
    }
}

impl<R> RepoBuilder<InMemoryStorage, R, AlwaysAnnounce> {
    pub fn new(runtime: R) -> RepoBuilder<InMemoryStorage, R, AlwaysAnnounce> {
        RepoBuilder {
            storage: InMemoryStorage::new(),
            runtime,
            peer_id: None,
            announce_policy: AlwaysAnnounce,
            concurrency: ConcurrencyConfig::AsyncRuntime,
            on_document_served: None,
        }
    }
}

impl<S: Storage, R: RuntimeHandle, A: AnnouncePolicy> RepoBuilder<S, R, A> {
    /// Create the repository
    pub async fn load(self) -> Repo {
        Repo::load(self).await
    }
}

impl<S: LocalStorage, R: LocalRuntimeHandle, A: LocalAnnouncePolicy> RepoBuilder<S, R, A> {
    /// Create the repository
    pub async fn load_local(self) -> Repo {
        Repo::load_local(self).await
    }
}
