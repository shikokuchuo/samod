use automerge::{ReadDoc, transaction::Transactable};
use samod_test_harness::{Network, RunningDocIds};

fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

/// When access is denied, `find_document` returns None (NotFound) on the
/// requesting side because the server sends DocUnavailable.
#[test]
fn denied_access_returns_not_found() {
    init_logging();
    let mut network = Network::new();

    let alice = network.create_samod("Alice");
    let bob = network.create_samod("Bob");

    // Alice denies all access and does not announce (no proactive push)
    network
        .samod(&alice)
        .set_announce_policy(Box::new(|_, _| false));
    network
        .samod(&alice)
        .set_access_policy(Box::new(|_doc_id, _peer_id| false));

    // Alice creates a document
    let RunningDocIds { doc_id, actor_id } = network.samod(&alice).create_document();

    network
        .samod(&alice)
        .with_document_by_actor(actor_id, |doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "key", "value").unwrap();
            tx.commit();
        })
        .expect("with document should succeed");

    network.connect(alice, bob);
    network.run_until_quiescent();

    // Bob tries to find the document — should get None (NotFound)
    let bob_actor_id = network.samod(&bob).find_document(&doc_id);
    assert!(
        bob_actor_id.is_none(),
        "Bob should not be able to access Alice's document when access is denied"
    );
}

/// When access is allowed (default AllowAll behavior), sync proceeds normally.
#[test]
fn allowed_access_proceeds_normally() {
    init_logging();
    let mut network = Network::new();

    let alice = network.create_samod("Alice");
    let bob = network.create_samod("Bob");

    // Default access policy is AllowAll, so no need to set anything

    let RunningDocIds { doc_id, actor_id } = network.samod(&alice).create_document();

    network
        .samod(&alice)
        .with_document_by_actor(actor_id, |doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "foo", "bar").unwrap();
            tx.commit();
        })
        .expect("with document should succeed");

    network.connect(alice, bob);
    network.run_until_quiescent();

    // Bob requests the document — should succeed
    let bob_actor_id = network
        .samod(&bob)
        .find_document(&doc_id)
        .expect("Bob should find Alice's document");

    let has_data = network
        .samod(&bob)
        .with_document_by_actor(bob_actor_id, |doc| {
            doc.get(automerge::ROOT, "foo")
                .unwrap()
                .map(|(v, _)| match v {
                    automerge::Value::Scalar(s) => match s.as_ref() {
                        automerge::ScalarValue::Str(string) => string == "bar",
                        _ => false,
                    },
                    _ => false,
                })
                .unwrap_or(false)
        })
        .expect("with document should succeed");

    assert!(has_data, "Bob should have Alice's data after allowed sync");
}

/// Access policy can selectively deny specific documents while allowing others.
/// Announce policy is set to not announce so Bob must explicitly request.
#[test]
fn selective_access_policy() {
    init_logging();
    let mut network = Network::new();

    let alice = network.create_samod("Alice");
    let bob = network.create_samod("Bob");

    // Alice creates two documents before setting policies
    let doc1 = network.samod(&alice).create_document();
    let doc2 = network.samod(&alice).create_document();

    network
        .samod(&alice)
        .with_document_by_actor(doc1.actor_id, |doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "doc", "one").unwrap();
            tx.commit();
        })
        .unwrap();

    network
        .samod(&alice)
        .with_document_by_actor(doc2.actor_id, |doc| {
            let mut tx = doc.transaction();
            tx.put(automerge::ROOT, "doc", "two").unwrap();
            tx.commit();
        })
        .unwrap();

    // Alice does not announce (so Bob must request), and denies access to doc1
    network
        .samod(&alice)
        .set_announce_policy(Box::new(|_, _| false));
    let denied_doc_id = doc1.doc_id.clone();
    network
        .samod(&alice)
        .set_access_policy(Box::new(move |doc_id, _peer_id| doc_id != denied_doc_id));

    network.connect(alice, bob);
    network.run_until_quiescent();

    // Bob should NOT find doc1
    let bob_doc1 = network.samod(&bob).find_document(&doc1.doc_id);
    assert!(
        bob_doc1.is_none(),
        "Bob should not be able to access denied doc1"
    );

    // Bob SHOULD find doc2
    let bob_doc2 = network.samod(&bob).find_document(&doc2.doc_id);
    assert!(
        bob_doc2.is_some(),
        "Bob should be able to access allowed doc2"
    );
}
