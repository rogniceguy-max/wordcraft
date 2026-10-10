//! A narrow Yrs spike for concurrent editing within one synthetic paragraph.
//!
//! This is deliberately not wired into `Session`: WordCraft's current undo stack stores whole
//! document snapshots and its paragraph paths are positional. The test checks whether Yrs can
//! provide a safer text-level primitive before a production adapter is designed.

use yrs::types::text::TextRef;
use yrs::undo::{Options, UndoManager};
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update};

const PARAGRAPH: &str = "paragraph:prototype-1";

fn seed_update() -> Vec<u8> {
    let seed = Doc::with_client_id(1);
    let paragraph = seed.get_or_insert_text(PARAGRAPH);
    paragraph.push(&mut seed.transact_mut(), "前後");
    seed.transact().encode_state_as_update_v1(&StateVector::default())
}

fn replica(client_id: u64, update: &[u8]) -> (Doc, TextRef) {
    let doc = Doc::with_client_id(client_id);
    let paragraph = doc.get_or_insert_text(PARAGRAPH);
    let decoded = Update::decode_v1(update).expect("seed update decodes");
    doc.transact_mut().apply_update(decoded).expect("seed update applies");
    (doc, paragraph)
}

fn apply(doc: &Doc, update: &[u8]) {
    let decoded = Update::decode_v1(update).expect("update decodes");
    doc.transact_mut().apply_update(decoded).expect("update applies");
}

fn sync(from: &Doc, to: &Doc) {
    let target_state = to.transact().state_vector();
    let update = from.transact().encode_state_as_update_v1(&target_state);
    apply(to, &update);
}

fn text(doc: &Doc, paragraph: &TextRef) -> String {
    paragraph.get_string(&doc.transact())
}

#[test]
fn concurrent_unicode_inserts_converge_in_either_delivery_order() {
    let seed = seed_update();
    let (alice, alice_text) = replica(2, &seed);
    let (bob, bob_text) = replica(3, &seed);

    // WordCraft Pos.off counts UTF-8 bytes. Yrs' default OffsetKind does too; after `前` is byte 3.
    let boundary_after_first_han = "前".len() as u32;
    let alice_update = {
        let mut txn = alice.transact_mut_with(alice.client_id());
        alice_text.insert(&mut txn, boundary_after_first_han, "甲👩‍💻");
        txn.encode_update_v1()
    };
    let bob_update = {
        let mut txn = bob.transact_mut_with(bob.client_id());
        bob_text.insert(&mut txn, boundary_after_first_han, "乙e\u{301}");
        txn.encode_update_v1()
    };

    // Apply the same concurrent updates in opposite orders on two fresh replicas.
    let (first, first_text) = replica(4, &seed);
    apply(&first, &alice_update);
    apply(&first, &bob_update);

    let (second, second_text) = replica(5, &seed);
    apply(&second, &bob_update);
    apply(&second, &alice_update);

    let first_value = text(&first, &first_text);
    let second_value = text(&second, &second_text);
    assert_eq!(first_value, second_value);
    assert!(
        first_value == "前甲👩‍💻乙e\u{301}後" || first_value == "前乙e\u{301}甲👩‍💻後",
        "concurrent inserts should preserve both Unicode chunks: {first_value:?}"
    );
    assert_eq!(text(&alice, &alice_text), "前甲👩‍💻後");
    assert_eq!(text(&bob, &bob_text), "前乙e\u{301}後");
}

#[test]
fn actor_scoped_undo_preserves_remote_insert_and_redo_converges() {
    let seed = seed_update();
    let (alice, alice_text) = replica(12, &seed);
    let (bob, bob_text) = replica(13, &seed);

    let mut alice_undo = UndoManager::<()>::with_options(Options::<()>::default());
    alice_undo.expand_scope(&alice, &alice_text);
    alice_undo.include_origin(alice.client_id());

    let at = "前".len() as u32;
    {
        let mut txn = alice.transact_mut_with(alice.client_id());
        alice_text.insert(&mut txn, at, "甲👩‍💻");
    }
    alice_undo.reset();
    {
        let mut txn = bob.transact_mut_with(bob.client_id());
        bob_text.insert(&mut txn, at, "乙e\u{301}");
    }

    // Each peer has one local insert and receives the other's independent update.
    sync(&alice, &bob);
    sync(&bob, &alice);
    let before_undo = text(&alice, &alice_text);
    assert_eq!(before_undo, text(&bob, &bob_text));

    assert!(alice_undo.undo_blocking());
    sync(&alice, &bob);
    let after_undo = text(&alice, &alice_text);
    assert_eq!(after_undo, "前乙e\u{301}後");
    assert_eq!(after_undo, text(&bob, &bob_text));

    assert!(alice_undo.redo_blocking());
    sync(&alice, &bob);
    assert_eq!(text(&alice, &alice_text), before_undo);
    assert_eq!(text(&alice, &alice_text), text(&bob, &bob_text));
}

#[test]
fn collaborative_byte_offset_projects_to_wordcraft_paragraph_edit() {
    let seed = seed_update();
    let (doc, paragraph) = replica(22, &seed);
    let offset = "前".len() as u32;
    {
        let mut txn = doc.transact_mut_with(doc.client_id());
        paragraph.insert(&mut txn, offset, "🧑🏽‍🚀");
    }

    let collaborative_text = text(&doc, &paragraph);
    assert_eq!(collaborative_text, "前🧑🏽‍🚀後");
    let byte_offset = usize::try_from(offset).expect("offset fits usize");
    assert!(collaborative_text.is_char_boundary(byte_offset));

    let mut document = wordcraft_doc::Document::from_text(&collaborative_text);
    let end = document
        .insert_text(&wordcraft_doc::Pos::body(0, byte_offset), "中", &wordcraft_doc::CharProps::default())
        .expect("byte-boundary projection inserts");
    assert_eq!(end.off, byte_offset + "中".len());
    assert_eq!(document.body[0].as_para().expect("paragraph").text, "前中🧑🏽‍🚀後");
}
