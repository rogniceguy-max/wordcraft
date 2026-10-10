//! Small, platform-independent rules used by the browser server bridge.

/// Keep a safety margin under the usual per-origin localStorage quota.
pub const MAX_LOCAL_DRAFT_BYTES: usize = 2 * 1024 * 1024;
const LOCAL_DRAFT_KEY_PREFIX: &str = "wordcraft-unsaved-document-v1:tab:";

pub fn local_draft_fits(bytes: usize) -> bool {
    bytes <= MAX_LOCAL_DRAFT_BYTES
}

/// Build the recovery key owned by one browser tab. An invalid identifier has no key, so callers
/// cannot accidentally fall back to the old origin-wide draft entry.
pub fn local_draft_storage_key(tab_id: &str) -> Option<String> {
    if tab_id.is_empty() || tab_id.len() > 128 || !tab_id.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-') {
        return None;
    }
    Some(format!("{LOCAL_DRAFT_KEY_PREFIX}{tab_id}"))
}

/// Resolve an optimistic version when a queued save is dispatched.
///
/// A create has no base version. An update can use the current version only if the active document
/// still matches the document captured by the queued action.
pub fn save_base_version(save_id: Option<&str>, active_id: Option<&str>, active_version: u64) -> Option<u64> {
    match save_id {
        None => Some(0),
        Some(id) if active_id == Some(id) => Some(active_version),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_LOCAL_DRAFT_BYTES, local_draft_fits, local_draft_storage_key, save_base_version};

    #[test]
    fn queued_update_uses_the_latest_version_for_the_same_document() {
        assert_eq!(save_base_version(Some("doc-1"), Some("doc-1"), 8), Some(8));
    }

    #[test]
    fn queued_save_is_rejected_if_the_active_document_changed() {
        assert_eq!(save_base_version(Some("doc-1"), Some("doc-2"), 8), None);
    }

    #[test]
    fn create_has_no_base_version() {
        assert_eq!(save_base_version(None, None, 8), Some(0));
    }

    #[test]
    fn local_draft_limit_is_two_mibibytes() {
        assert_eq!(MAX_LOCAL_DRAFT_BYTES, 2 * 1024 * 1024);
    }

    #[test]
    fn local_draft_size_check_includes_the_limit_and_rejects_larger_payloads() {
        assert!(local_draft_fits(MAX_LOCAL_DRAFT_BYTES));
        assert!(!local_draft_fits(MAX_LOCAL_DRAFT_BYTES + 1));
    }

    #[test]
    fn local_draft_keys_are_isolated_by_tab_id() {
        assert_ne!(local_draft_storage_key("tab-one"), local_draft_storage_key("tab-two"));
        assert_eq!(local_draft_storage_key("tab-one").as_deref(), Some("wordcraft-unsaved-document-v1:tab:tab-one"));
    }

    #[test]
    fn invalid_tab_id_has_no_local_draft_key_or_shared_key_fallback() {
        assert_eq!(local_draft_storage_key(""), None);
        assert_eq!(local_draft_storage_key("../shared"), None);
        assert_eq!(local_draft_storage_key(&"a".repeat(129)), None);
    }
}
