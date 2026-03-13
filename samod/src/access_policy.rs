use samod_core::{DocumentId, PeerId};

/// Whether to allow a peer to sync a document
///
/// To configure access control implement this trait and pass the
/// implementation to
/// [`RepoBuilder::with_access_policy`](crate::RepoBuilder::with_access_policy).
/// Note that the trait is implemented for `Fn(DocumentId, PeerId) -> bool`, so
/// a closure can be passed directly in many cases.
pub trait AccessPolicy: Clone + Send + 'static {
    /// Whether we should allow the given peer to sync the given document.
    ///
    /// When a peer sends a `request` or `sync` message for a document, this
    /// method is called. If it returns `false`, the peer receives a
    /// `doc-unavailable` response and no sync data is sent.
    ///
    /// Note that the peer IDs are not authenticated by the network protocol
    /// `samod` implements, so if you are relying on this method for
    /// authorization you must make sure that the network layer you provide is
    /// doing authentication in its own fashion somehow.
    fn should_allow(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
    ) -> impl Future<Output = bool> + Send + 'static;
}

/// A version of [`AccessPolicy`] that can be used with runtimes that don't
/// require `Send` or `'static` bounds. See the [module level documentation on
/// runtimes](./index.html#runtimes) for more details.
pub trait LocalAccessPolicy: Clone + 'static {
    fn should_allow(&self, doc_id: DocumentId, peer_id: PeerId) -> impl Future<Output = bool>;
}

impl<A: AccessPolicy> LocalAccessPolicy for A {
    fn should_allow(&self, doc_id: DocumentId, peer_id: PeerId) -> impl Future<Output = bool> {
        AccessPolicy::should_allow(self, doc_id, peer_id)
    }
}

impl<F> AccessPolicy for F
where
    F: Fn(DocumentId, PeerId) -> bool + Clone + Send + 'static,
{
    fn should_allow(
        &self,
        doc_id: DocumentId,
        peer_id: PeerId,
    ) -> impl Future<Output = bool> + Send + 'static {
        let result = self(doc_id, peer_id);
        async move { result }
    }
}

/// Allow all peers to sync all documents (default behavior)
#[derive(Clone)]
pub struct AllowAll;

impl AccessPolicy for AllowAll {
    #[allow(clippy::manual_async_fn)]
    fn should_allow(
        &self,
        _doc_id: DocumentId,
        _peer_id: PeerId,
    ) -> impl Future<Output = bool> + Send + 'static {
        async { true }
    }
}
