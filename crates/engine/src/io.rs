//! File format dispatch by extension. Format crates plug in here.

use wordcraft_doc::Document;

/// Versioned JSON envelope used for server-side document storage. This is separate from the
/// user-facing `.json` export: the latter mirrors `Document`'s serde shape for compatibility.
pub const STORAGE_FORMAT: &str = "wordcraft.document";
pub const STORAGE_FORMAT_VERSION: u32 = 1;

/// Formats WordCraft opens.
pub const OPEN_EXTS: &[&str] = &["docx", "docm", "dotx", "txt", "md", "markdown", "html", "htm", "rtf", "odt", "wcraft.json", "json"];
/// Formats WordCraft saves (Save As).
pub const SAVE_EXTS: &[&str] = &["docx", "pdf", "txt", "md", "html", "rtf", "odt", "png", "json"];

fn ext_of(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    lower.rsplit('.').next().unwrap_or("").to_string()
}

/// Parse a document from bytes; `name` gives the format by extension.
pub fn open_bytes(name: &str, bytes: &[u8]) -> Result<Document, String> {
    let ext = ext_of(name);
    let mut doc = match ext.as_str() {
        "txt" | "text" | "" => Document::from_text(&decode_text(bytes)),
        "json" => serde_json::from_slice::<Document>(bytes).map_err(|e| format!("{name}: {e}"))?,
        other => match crate::io_ext::open(other, bytes) {
            Some(r) => r?,
            None => return Err(format!("{name}: unsupported format `.{other}`")),
        },
    };
    doc.ensure_nonempty();
    Ok(doc)
}

/// Serialise a document; `name` gives the format by extension.
pub fn save_bytes(name: &str, doc: &Document) -> Result<Vec<u8>, String> {
    let ext = ext_of(name);
    match ext.as_str() {
        "txt" | "text" => Ok(doc.plain_text(wordcraft_doc::StoryRef::Body).replace('\n', "\r\n").into_bytes()),
        "json" => serde_json::to_vec_pretty(doc).map_err(|e| e.to_string()),
        other => match crate::io_ext::save(other, doc) {
            Some(r) => r,
            None => Err(format!("{name}: can't save as `.{other}`")),
        },
    }
}

/// Render one document page as a PNG at `scale` pixels per point.
pub fn render_png(doc: &Document, page: usize, scale: f32) -> Result<Vec<u8>, String> {
    crate::io_ext::render_png(doc, page, scale)
}

/// Serialize a complete document snapshot for persistent server storage.
///
/// `Document::media` and `Document::passthrough` are intentionally skipped by ordinary serde
/// serialization. The storage envelope carries them explicitly so images and preserved source
/// parts survive reloads and version snapshots.
pub fn storage_snapshot(doc: &Document) -> Result<serde_json::Value, String> {
    let media = doc
        .media
        .iter()
        .map(|(key, bytes)| (key.clone(), serde_json::Value::String(crate::cmd::insert::base64_encode(bytes))))
        .collect::<serde_json::Map<_, _>>();
    let passthrough = doc
        .passthrough
        .iter()
        .map(|(key, bytes)| (key.clone(), serde_json::Value::String(crate::cmd::insert::base64_encode(bytes))))
        .collect::<serde_json::Map<_, _>>();
    let document = serde_json::to_value(doc).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({
        "format": STORAGE_FORMAT,
        "formatVersion": STORAGE_FORMAT_VERSION,
        "document": document,
        "media": media,
        "passthrough": passthrough,
    }))
}

/// Restore a complete document from a versioned server-storage snapshot.
pub fn document_from_storage_snapshot(snapshot: &serde_json::Value) -> Result<Document, String> {
    if snapshot.get("format").and_then(serde_json::Value::as_str) != Some(STORAGE_FORMAT) {
        return Err("unrecognized WordCraft storage snapshot".into());
    }
    if snapshot.get("formatVersion").and_then(serde_json::Value::as_u64) != Some(u64::from(STORAGE_FORMAT_VERSION)) {
        return Err("unsupported WordCraft storage snapshot version".into());
    }
    let mut doc = serde_json::from_value::<Document>(snapshot.get("document").cloned().ok_or("snapshot is missing document data")?)
        .map_err(|e| format!("invalid document data: {e}"))?;
    doc.media = decode_storage_blobs(snapshot.get("media"), "media")?;
    doc.passthrough = decode_storage_blobs(snapshot.get("passthrough"), "passthrough")?;
    doc.ensure_nonempty();
    Ok(doc)
}

fn decode_storage_blobs(
    value: Option<&serde_json::Value>,
    field: &str,
) -> Result<std::collections::BTreeMap<String, std::sync::Arc<Vec<u8>>>, String> {
    let mut blobs = std::collections::BTreeMap::new();
    let Some(value) = value else { return Ok(blobs) };
    let entries = value.as_object().ok_or_else(|| format!("snapshot {field} must be an object"))?;
    for (key, value) in entries {
        let encoded = value.as_str().ok_or_else(|| format!("snapshot {field} entry `{key}` must be a string"))?;
        let bytes = crate::cmd::insert::base64_decode(encoded).ok_or_else(|| format!("snapshot {field} entry `{key}` is not valid base64"))?;
        blobs.insert(key.clone(), std::sync::Arc::new(bytes));
    }
    Ok(blobs)
}

/// Serialize a complete document snapshot as UTF-8 JSON bytes.
pub fn storage_snapshot_bytes(doc: &Document) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&storage_snapshot(doc)?).map_err(|e| e.to_string())
}

/// Restore a complete document from UTF-8 JSON bytes.
pub fn document_from_storage_bytes(bytes: &[u8]) -> Result<Document, String> {
    let snapshot = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    document_from_storage_snapshot(&snapshot)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn open_path(path: &std::path::Path) -> Result<Document, String> {
    let meta = std::fs::metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if meta.len() > 2 << 30 {
        return Err(format!("{}: file is larger than 2 GB", path.display()));
    }
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    open_bytes(&path.to_string_lossy(), &bytes)
}

#[cfg(target_arch = "wasm32")]
pub fn open_path(path: &std::path::Path) -> Result<Document, String> {
    Err(format!("{}: files are opened through the browser on the web", path.display()))
}

#[cfg(not(target_arch = "wasm32"))]
pub fn save_path(path: &std::path::Path, doc: &Document) -> Result<(), String> {
    let bytes = save_bytes(&path.to_string_lossy(), doc)?;
    // Write atomically: temp file next to the target, then rename.
    let tmp = path.with_extension(format!("{}.tmp", ext_of(&path.to_string_lossy())));
    std::fs::write(&tmp, &bytes).map_err(|e| format!("{}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(target_arch = "wasm32")]
pub fn save_path(path: &std::path::Path, _doc: &Document) -> Result<(), String> {
    Err(format!("{}: files are saved through the browser on the web", path.display()))
}

/// UTF-8 (with or without BOM), UTF-16 with BOM, else Latin-1.
pub fn decode_text(b: &[u8]) -> String {
    if let Some(rest) = b.strip_prefix(&[0xEF, 0xBB, 0xBF]) {
        return String::from_utf8_lossy(rest).into_owned();
    }
    if b.len() >= 2 && (b[0] == 0xFF && b[1] == 0xFE || b[0] == 0xFE && b[1] == 0xFF) {
        let le = b[0] == 0xFF;
        let units: Vec<u16> =
            b[2..].as_chunks::<2>().0.iter().map(|c| if le { u16::from_le_bytes([c[0], c[1]]) } else { u16::from_be_bytes([c[0], c[1]]) }).collect();
        return String::from_utf16_lossy(&units);
    }
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => b.iter().map(|c| *c as char).collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{document_from_storage_snapshot, storage_snapshot};

    #[test]
    fn storage_snapshot_preserves_media_and_passthrough() {
        let mut doc = wordcraft_doc::Document::from_text("A shared document");
        doc.media.insert("image.png".into(), Arc::new(vec![0, 1, 127, 255]));
        doc.passthrough.insert("custom.xml".into(), Arc::new(b"<custom/>".to_vec()));

        let snapshot = storage_snapshot(&doc).expect("snapshot serializes");
        let restored = document_from_storage_snapshot(&snapshot).expect("snapshot restores");

        assert_eq!(restored, doc);
    }

    #[test]
    fn storage_snapshot_rejects_unknown_version() {
        let mut snapshot = storage_snapshot(&wordcraft_doc::Document::new()).expect("snapshot serializes");
        snapshot["formatVersion"] = serde_json::json!(2);
        assert!(document_from_storage_snapshot(&snapshot).is_err());
    }
}
