//! Verified online backups for WordCraft's single-file SQLite store.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unimplemented, clippy::todo, clippy::unreachable)]

use std::collections::{BTreeMap, HashSet};
use std::env;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::backup::Backup;
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

const DATABASE_FILE: &str = "wordcraft.sqlite3";
const MANIFEST_FILE: &str = "manifest.json";
const SQLITE_SIDECAR_SUFFIXES: &[&str] = &["-wal", "-shm", "-journal"];
const BACKUP_PREFIX: &str = "daily-";
const BACKUP_SUFFIX: &str = ".backup";
const MANIFEST_SCHEMA: u32 = 1;
const KEEP_DAILY: usize = 14;
const KEEP_WEEKLY: usize = 8;

#[derive(Debug, Error)]
enum BackupError {
    #[error("usage: {0}")]
    Usage(String),
    #[error("backup verification failed: {0}")]
    Verification(String),
    #[error("backup cleanup could not remove one or more expired backups")]
    Cleanup,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, BackupError>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BackupManifest {
    schema_version: u32,
    backup_type: String,
    created_at_unix_seconds: i64,
    created_at_utc: String,
    database_file: String,
    database_sha256: String,
    document_count: u64,
    document_version_count: u64,
    wordcraft_version: String,
}

#[derive(Clone, Debug)]
struct VerifiedBackup {
    directory: PathBuf,
    manifest: BackupManifest,
}

#[derive(Clone, Copy, Debug)]
struct DatabaseSummary {
    documents: u64,
    versions: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("wordcraft-backup: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut arguments = env::args_os();
    let _program = arguments.next();
    let command = arguments.next().and_then(|value| value.into_string().ok()).ok_or_else(|| BackupError::Usage(usage().to_string()))?;
    let rest = arguments.collect::<Vec<_>>();

    match command.as_str() {
        "daily" => {
            require_at_most(&rest, 1, "daily [BACKUP_DIR]")?;
            let backup_dir = match rest.first() {
                Some(path) => PathBuf::from(path),
                None => default_backup_dir(),
            };
            daily(&backup_dir)
        }
        "list" => {
            require_at_most(&rest, 1, "list [BACKUP_DIR]")?;
            let backup_dir = match rest.first() {
                Some(path) => PathBuf::from(path),
                None => default_backup_dir(),
            };
            list_backups(&backup_dir)
        }
        "verify" => {
            require_exact(&rest, 1, "verify BACKUP_DIR")?;
            let verified = verify_backup(Path::new(&rest[0]))?;
            print_verified(&verified);
            Ok(())
        }
        "restore" => restore_command(&rest),
        "--help" | "-h" | "help" => {
            println!("{}", usage());
            Ok(())
        }
        _ => Err(BackupError::Usage(usage().to_string())),
    }
}

fn usage() -> &'static str {
    "wordcraft-backup daily [BACKUP_DIR] | list [BACKUP_DIR] | verify BACKUP_DIR | restore BACKUP_DIR --target-dir DIR | restore BACKUP_DIR --target-db DB"
}

fn require_exact(arguments: &[OsString], expected: usize, form: &str) -> Result<()> {
    if arguments.len() == expected { Ok(()) } else { Err(BackupError::Usage(form.to_string())) }
}

fn require_at_most(arguments: &[OsString], maximum: usize, form: &str) -> Result<()> {
    if arguments.len() <= maximum { Ok(()) } else { Err(BackupError::Usage(form.to_string())) }
}

fn default_data_dir() -> PathBuf {
    match env::var_os("WORDCRAFT_DATA_DIR") {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from("data"),
    }
}

fn default_backup_dir() -> PathBuf {
    match env::var_os("WORDCRAFT_BACKUP_DIR") {
        Some(path) => PathBuf::from(path),
        None => default_data_dir().join("backups"),
    }
}

fn daily(backup_dir: &Path) -> Result<()> {
    let source_path = default_data_dir().join(DATABASE_FILE);
    if !source_path.is_file() {
        return Err(BackupError::Verification(format!("source database does not exist: {}", source_path.display())));
    }
    fs::create_dir_all(backup_dir)?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| BackupError::Verification(format!("system clock is before the Unix epoch: {error}")))?;
    let seconds = i64::try_from(now.as_secs()).map_err(|_| BackupError::Verification("system time is out of range".to_string()))?;
    let stamp = format_timestamp(seconds).replace(':', "-");
    let backup_name = format!("{BACKUP_PREFIX}{stamp}-{:09}{BACKUP_SUFFIX}", now.subsec_nanos());
    let final_directory = backup_dir.join(backup_name);
    if final_directory.exists() {
        return Err(BackupError::Verification(format!("backup already exists: {}", final_directory.display())));
    }

    let staging_directory = backup_dir.join(format!(".wordcraft-backup-{}-{}-{:09}.tmp", std::process::id(), seconds, now.subsec_nanos()));
    fs::create_dir(&staging_directory)?;

    let staged_result = (|| {
        let database_path = staging_directory.join(DATABASE_FILE);
        create_online_backup(&source_path, &database_path)?;
        let summary = verify_database(&database_path)?;
        let digest = sha256_file(&database_path)?;
        let manifest = BackupManifest {
            schema_version: MANIFEST_SCHEMA,
            backup_type: "daily".to_string(),
            created_at_unix_seconds: seconds,
            created_at_utc: format_timestamp(seconds),
            database_file: DATABASE_FILE.to_string(),
            database_sha256: digest,
            document_count: summary.documents,
            document_version_count: summary.versions,
            wordcraft_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let manifest_path = staging_directory.join(MANIFEST_FILE);
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        let mut file = OpenOptions::new().write(true).create_new(true).open(&manifest_path)?;
        file.write_all(&manifest_bytes)?;
        file.sync_all()?;
        sync_directory(&staging_directory)?;
        verify_backup(&staging_directory)?;
        Ok::<(), BackupError>(())
    })();

    if let Err(error) = staged_result {
        cleanup_staging_directory(&staging_directory);
        return Err(error);
    }

    if let Err(error) = fs::rename(&staging_directory, &final_directory) {
        eprintln!("could not publish backup directory {}: {error}", final_directory.display());
        cleanup_staging_directory(&staging_directory);
        return Err(error.into());
    }
    sync_directory(backup_dir)?;
    println!("Created and verified backup: {}", final_directory.display());

    prune_backups(backup_dir)
}

fn create_online_backup(source_path: &Path, destination_path: &Path) -> Result<()> {
    let source = Connection::open_with_flags(source_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    source.busy_timeout(Duration::from_secs(30))?;
    let mut destination = Connection::open(destination_path)?;
    destination.busy_timeout(Duration::from_secs(30))?;
    let backup = Backup::new(&source, &mut destination)?;
    backup.run_to_completion(64, Duration::from_millis(50), None)?;
    drop(backup);
    drop(destination);
    let normalized = Connection::open(destination_path)?;
    let mode = normalized.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get::<_, String>(0))?;
    if !mode.eq_ignore_ascii_case("delete") {
        return Err(BackupError::Verification(format!("backup database did not leave WAL mode: {mode}")));
    }
    drop(normalized);
    remove_sqlite_sidecars(destination_path)?;
    OpenOptions::new().read(true).write(true).open(destination_path)?.sync_all()?;
    Ok(())
}

fn verify_backup(directory: &Path) -> Result<VerifiedBackup> {
    if !directory.is_dir() {
        return Err(BackupError::Verification(format!("backup directory does not exist: {}", directory.display())));
    }
    let manifest_path = directory.join(MANIFEST_FILE);
    let manifest: BackupManifest = serde_json::from_reader(File::open(&manifest_path)?)?;
    if manifest.schema_version != MANIFEST_SCHEMA {
        return Err(BackupError::Verification(format!("unsupported manifest schema {} in {}", manifest.schema_version, manifest_path.display())));
    }
    if manifest.backup_type != "daily" || manifest.database_file != DATABASE_FILE {
        return Err(BackupError::Verification(format!("unexpected manifest metadata in {}", manifest_path.display())));
    }

    let database_path = directory.join(DATABASE_FILE);
    let actual_digest = sha256_file(&database_path)?;
    if actual_digest != manifest.database_sha256 {
        return Err(BackupError::Verification(format!(
            "SHA-256 mismatch for {} (manifest {}, actual {})",
            database_path.display(),
            manifest.database_sha256,
            actual_digest
        )));
    }
    let summary = verify_database(&database_path)?;
    if summary.documents != manifest.document_count || summary.versions != manifest.document_version_count {
        return Err(BackupError::Verification(format!(
            "snapshot counts do not match manifest for {} (documents {}/{}, versions {}/{})",
            database_path.display(),
            summary.documents,
            manifest.document_count,
            summary.versions,
            manifest.document_version_count
        )));
    }
    Ok(VerifiedBackup { directory: directory.to_path_buf(), manifest })
}

fn verify_database(database_path: &Path) -> Result<DatabaseSummary> {
    let mut connection = Connection::open_with_flags(database_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(Duration::from_secs(30))?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
    verify_integrity(&transaction)?;
    verify_foreign_keys(&transaction)?;
    let summary = validate_stored_snapshots(&transaction)?;
    transaction.commit()?;
    Ok(summary)
}

fn verify_integrity(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA integrity_check")?;
    let mut rows = statement.query([])?;
    let mut row_count = 0_u64;
    let mut failures = Vec::new();
    while let Some(row) = rows.next()? {
        row_count = row_count.saturating_add(1);
        let message: String = row.get(0)?;
        if message != "ok" && failures.len() < 10 {
            failures.push(message);
        }
    }
    if row_count != 1 || !failures.is_empty() {
        return Err(BackupError::Verification(format!(
            "SQLite integrity_check returned {} row(s): {}",
            row_count,
            if failures.is_empty() { "expected exactly one 'ok' row".to_string() } else { failures.join("; ") }
        )));
    }
    Ok(())
}

fn verify_foreign_keys(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
    let mut rows = statement.query([])?;
    if let Some(row) = rows.next()? {
        let table: String = row.get(0)?;
        let row_id: Option<i64> = row.get(1)?;
        let parent: String = row.get(2)?;
        return Err(BackupError::Verification(format!("foreign-key violation in table {table}, row {row_id:?}, parent {parent}")));
    }
    Ok(())
}

fn validate_stored_snapshots(connection: &Connection) -> Result<DatabaseSummary> {
    let documents = read_document_snapshots(connection)?;
    let versions = read_version_snapshots(connection)?;
    Ok(DatabaseSummary { documents, versions })
}

fn read_document_snapshots(connection: &Connection) -> Result<u64> {
    let mut statement = connection.prepare("SELECT id, snapshot FROM documents ORDER BY id")?;
    let mut rows = statement.query([])?;
    let mut count = 0_u64;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let bytes: Vec<u8> = row.get(1)?;
        let context = format!("documents/{id}");
        validate_snapshot(&bytes, &context)?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn read_version_snapshots(connection: &Connection) -> Result<u64> {
    let mut statement = connection.prepare("SELECT document_id, version, snapshot FROM document_versions ORDER BY document_id, version")?;
    let mut rows = statement.query([])?;
    let mut count = 0_u64;
    while let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let version: i64 = row.get(1)?;
        let bytes: Vec<u8> = row.get(2)?;
        let context = format!("document_versions/{id}/{version}");
        validate_snapshot(&bytes, &context)?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn validate_snapshot(bytes: &[u8], context: &str) -> Result<()> {
    let snapshot: Value =
        serde_json::from_slice(bytes).map_err(|error| BackupError::Verification(format!("{context} contains invalid snapshot JSON: {error}")))?;
    wordcraft_engine::io::document_from_storage_snapshot(&snapshot)
        .map(|_| ())
        .map_err(|error| BackupError::Verification(format!("{context} contains an invalid storage snapshot: {error}")))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn list_backups(backup_dir: &Path) -> Result<()> {
    if !backup_dir.is_dir() {
        return Err(BackupError::Verification(format!("backup directory does not exist: {}", backup_dir.display())));
    }
    let mut entries = fs::read_dir(backup_dir)?.collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    let mut found = false;
    for entry in entries {
        if !is_backup_directory(&entry.path()) {
            continue;
        }
        found = true;
        match verify_backup(&entry.path()) {
            Ok(verified) => print_verified(&verified),
            Err(error) => println!("FAILED  {}  {error}", entry.path().display()),
        }
    }
    if !found {
        println!("No WordCraft daily backups found in {}", backup_dir.display());
    }
    Ok(())
}

fn print_verified(backup: &VerifiedBackup) {
    println!(
        "VERIFIED  {}  {}  documents={} versions={} sha256={}",
        backup.directory.display(),
        backup.manifest.created_at_utc,
        backup.manifest.document_count,
        backup.manifest.document_version_count,
        backup.manifest.database_sha256
    );
}

fn is_backup_directory(path: &Path) -> bool {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(_) => return false,
    };
    if !metadata.file_type().is_dir() {
        return false;
    }
    let Some(name) = path.file_name() else {
        return false;
    };
    let name = name.to_string_lossy();
    name.starts_with(BACKUP_PREFIX) && name.ends_with(BACKUP_SUFFIX)
}

fn prune_backups(backup_dir: &Path) -> Result<()> {
    let mut verified = Vec::new();
    let entries = fs::read_dir(backup_dir)?.collect::<std::result::Result<Vec<_>, _>>()?;
    for entry in entries {
        let path = entry.path();
        if !is_backup_directory(&path) {
            continue;
        }
        match verify_backup(&path) {
            Ok(backup) => verified.push(backup),
            Err(error) => eprintln!("retention kept unverified backup {}: {error}", path.display()),
        }
    }
    verified.sort_by(|left, right| {
        right.manifest.created_at_unix_seconds.cmp(&left.manifest.created_at_unix_seconds).then_with(|| right.directory.cmp(&left.directory))
    });
    if verified.is_empty() {
        eprintln!("retention skipped: there is no verified backup to protect");
        return Ok(());
    }

    let mut keep = HashSet::new();
    for backup in verified.iter().take(KEEP_DAILY) {
        keep.insert(backup.directory.clone());
    }

    let mut weekly: BTreeMap<i64, &VerifiedBackup> = BTreeMap::new();
    for backup in verified.iter().skip(KEEP_DAILY) {
        let day = backup.manifest.created_at_unix_seconds.div_euclid(86_400);
        let week = (day + 3).div_euclid(7);
        weekly.entry(week).or_insert(backup);
    }
    for backup in weekly.into_values().rev().take(KEEP_WEEKLY) {
        keep.insert(backup.directory.clone());
    }

    let mut cleanup_failed = false;
    let mut verified_remaining = verified.len();
    for backup in verified.iter().filter(|backup| !keep.contains(&backup.directory)) {
        if verified_remaining <= 1 {
            eprintln!("retention kept the last verified backup: {}", backup.directory.display());
            continue;
        }
        if let Err(error) = remove_verified_backup(backup) {
            cleanup_failed = true;
            eprintln!("retention could not remove {}: {error}", backup.directory.display());
            continue;
        }
        verified_remaining = verified_remaining.saturating_sub(1);
    }
    if cleanup_failed { Err(BackupError::Cleanup) } else { Ok(()) }
}

fn remove_verified_backup(backup: &VerifiedBackup) -> Result<()> {
    remove_verified_backup_with(backup, |path| fs::remove_dir_all(path))
}

fn remove_verified_backup_with(backup: &VerifiedBackup, remove_directory: impl FnOnce(&Path) -> std::io::Result<()>) -> Result<()> {
    let parent = backup
        .directory
        .parent()
        .ok_or_else(|| BackupError::Verification(format!("backup has no parent directory: {}", backup.directory.display())))?;
    let quarantine_directory = parent.join(format!(".wordcraft-prune-{}-{}.tmp", std::process::id(), unique_suffix()?));
    if quarantine_directory.exists() {
        return Err(BackupError::Verification(format!("backup cleanup path already exists: {}", quarantine_directory.display())));
    }

    // Move the complete, verified backup out of the discoverable `daily-*.backup` namespace
    // before deleting it. A crash or partial deletion can then never leave a damaged backup
    // looking like a normal retained backup.
    fs::rename(&backup.directory, &quarantine_directory)?;
    sync_directory(parent)?;
    match remove_directory(&quarantine_directory) {
        Ok(()) => {
            sync_directory(parent)?;
            Ok(())
        }
        Err(error) => {
            match verify_backup(&quarantine_directory) {
                Ok(_) => eprintln!("retention could not remove verified backup; it is recoverable at {}: {error}", quarantine_directory.display()),
                Err(verification_error) => eprintln!(
                    "retention cleanup left an incomplete backup quarantined at {}: {error}; verification: {verification_error}",
                    quarantine_directory.display()
                ),
            }
            Err(error.into())
        }
    }
}

fn publish_restore_no_clobber(temporary_database: &Path, target_database: &Path) -> Result<()> {
    if let Err(error) = fs::hard_link(temporary_database, target_database) {
        if let Err(cleanup_error) = fs::remove_file(temporary_database) {
            eprintln!("could not remove incomplete restore database {}: {cleanup_error}", temporary_database.display());
        }
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(BackupError::Verification(format!("refusing to overwrite existing target: {}", target_database.display())));
        }
        return Err(error.into());
    }

    // The hard link publishes the complete file atomically and fails if another process created
    // the target after restore's initial existence check. Once linked, cleanup failure only leaves
    // a redundant temporary name; the target itself is already safely published.
    if let Err(error) = fs::remove_file(temporary_database) {
        eprintln!("could not remove temporary restore link {}: {error}", temporary_database.display());
    }
    let parent = target_database.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or(Path::new("."));
    sync_directory(parent)?;
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn cleanup_staging_directory(path: &Path) {
    let database = path.join(DATABASE_FILE);
    let mut files = vec![path.join(MANIFEST_FILE), database.clone()];
    files.extend(SQLITE_SIDECAR_SUFFIXES.iter().map(|suffix| sqlite_sidecar_path(&database, suffix)));
    for file in files {
        if file.exists()
            && let Err(error) = fs::remove_file(&file)
        {
            eprintln!("could not remove incomplete backup file {}: {error}", file.display());
        }
    }
    if path.exists()
        && let Err(error) = fs::remove_dir(path)
    {
        eprintln!("could not remove incomplete backup directory {}: {error}", path.display());
    }
}

fn sqlite_sidecar_path(database_path: &Path, suffix: &str) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

fn remove_sqlite_sidecars(database_path: &Path) -> Result<()> {
    for suffix in SQLITE_SIDECAR_SUFFIXES {
        let path = sqlite_sidecar_path(database_path, suffix);
        if path.exists() {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}

fn restore_command(arguments: &[OsString]) -> Result<()> {
    if arguments.len() != 3 {
        return Err(BackupError::Usage("restore BACKUP_DIR --target-dir DIR | restore BACKUP_DIR --target-db DB".to_string()));
    }
    let backup_directory = PathBuf::from(&arguments[0]);
    let mode = arguments[1].to_str().ok_or_else(|| BackupError::Usage("restore target option must be valid UTF-8".to_string()))?;
    let target = PathBuf::from(&arguments[2]);
    let target_database = match mode {
        "--target-dir" => target.join(DATABASE_FILE),
        "--target-db" => target,
        _ => return Err(BackupError::Usage("restore BACKUP_DIR --target-dir DIR | restore BACKUP_DIR --target-db DB".to_string())),
    };
    restore(&backup_directory, &target_database)
}

fn restore(backup_directory: &Path, target_database: &Path) -> Result<()> {
    let verified = verify_backup(backup_directory)?;
    if target_database.exists() && target_database.is_dir() {
        return Err(BackupError::Verification(format!("target database path is a directory: {}", target_database.display())));
    }
    if target_database.exists() {
        return Err(BackupError::Verification(format!("refusing to overwrite existing target: {}", target_database.display())));
    }
    let parent = match target_database.parent().filter(|path| !path.as_os_str().is_empty()) {
        Some(path) => path,
        None => Path::new("."),
    };
    fs::create_dir_all(parent)?;
    let canonical_backup = backup_directory.canonicalize()?;
    let canonical_parent = parent.canonicalize()?;
    if canonical_parent.starts_with(&canonical_backup) {
        return Err(BackupError::Verification("restore target must be outside the backup directory".to_string()));
    }

    let target_name = target_database.file_name().ok_or_else(|| BackupError::Verification("target database path must name a file".to_string()))?;
    let mut temp_name = OsString::from(".");
    temp_name.push(target_name);
    temp_name.push(format!(".restore-{}-{}.tmp", std::process::id(), unique_suffix()?));
    let temporary_database = parent.join(temp_name);
    let mut placeholder = OpenOptions::new().write(true).create_new(true).open(&temporary_database)?;
    placeholder.flush()?;
    drop(placeholder);

    let restore_result = (|| {
        create_online_backup(&backup_directory.join(DATABASE_FILE), &temporary_database)?;
        let summary = verify_database(&temporary_database)?;
        if summary.documents != verified.manifest.document_count || summary.versions != verified.manifest.document_version_count {
            return Err(BackupError::Verification("restored snapshot counts do not match the verified backup".to_string()));
        }
        Ok::<(), BackupError>(())
    })();
    if let Err(error) = restore_result {
        if let Err(cleanup_error) = fs::remove_file(&temporary_database) {
            eprintln!("could not remove incomplete restore database {}: {cleanup_error}", temporary_database.display());
        }
        return Err(error);
    }
    publish_restore_no_clobber(&temporary_database, target_database)?;
    println!("Restored verified backup to {}", target_database.display());
    Ok(())
}

fn unique_suffix() -> Result<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|error| BackupError::Verification(format!("system clock is before the Unix epoch: {error}")))
}

fn format_timestamp(seconds: i64) -> String {
    let day = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, date) = civil_from_days(day);
    let hour = second_of_day / 3_600;
    let minute = (second_of_day % 3_600) / 60;
    let second = second_of_day % 60;
    format!("{year:04}-{month:02}-{date:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let shifted_days = days_since_epoch + 719_468;
    let era = shifted_days.div_euclid(146_097);
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year_base = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let date = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year_base + if month <= 2 { 1 } else { 0 };
    (year, month, date)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use rusqlite::{Connection, params};
    use wordcraft_doc::{Document, StoryRef};

    use super::{
        BackupError, BackupManifest, DATABASE_FILE, MANIFEST_FILE, MANIFEST_SCHEMA, SQLITE_SIDECAR_SUFFIXES, create_online_backup, format_timestamp,
        is_backup_directory, publish_restore_no_clobber, remove_verified_backup_with, restore, sha256_file, sqlite_sidecar_path, verify_backup,
        verify_database,
    };

    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new(label: &str) -> Self {
            let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("system clock is after the Unix epoch").as_nanos();
            let path = std::env::temp_dir().join(format!("wordcraft-backup-{label}-{}-{nonce}", std::process::id()));
            std::fs::create_dir_all(&path).expect("scratch directory is created");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0) {
                eprintln!("could not remove test scratch directory {}: {error}", self.0.display());
            }
        }
    }

    fn create_sample_database(path: &Path) {
        let mut document = Document::from_text("Snapshot content preserved by backup.");
        document.media.insert("image.png".to_string(), Arc::new(vec![0, 3, 42, 255]));
        document.passthrough.insert("custom.xml".to_string(), Arc::new(b"<custom/>".to_vec()));
        let snapshot = wordcraft_engine::io::storage_snapshot(&document).expect("document snapshot is encoded");
        let snapshot_bytes = serde_json::to_vec(&snapshot).expect("snapshot JSON is encoded");

        let connection = Connection::open(path).expect("sample database opens");
        connection.pragma_update(None, "journal_mode", "WAL").expect("sample database uses WAL mode");
        connection
            .execute_batch(
                "CREATE TABLE documents (
                    id TEXT PRIMARY KEY NOT NULL,
                    title TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version >= 1),
                    created_at INTEGER NOT NULL,
                    modified_at INTEGER NOT NULL,
                    deleted_at INTEGER,
                    snapshot BLOB NOT NULL,
                    format_version INTEGER NOT NULL
                );
                CREATE TABLE document_versions (
                    document_id TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version >= 1),
                    title TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    snapshot BLOB NOT NULL,
                    format_version INTEGER NOT NULL,
                    PRIMARY KEY(document_id, version),
                    FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE
                );
                CREATE INDEX documents_active_modified ON documents(deleted_at, modified_at DESC);",
            )
            .expect("sample schema is created");
        connection
            .execute(
                "INSERT INTO documents(id, title, version, created_at, modified_at, deleted_at, snapshot, format_version)
                 VALUES(?1, ?2, 1, ?3, ?3, NULL, ?4, ?5)",
                params![
                    "sample-document",
                    "Backup sample",
                    1_800_000_000_000_i64,
                    &snapshot_bytes,
                    i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION)
                ],
            )
            .expect("current document snapshot is inserted");
        connection
            .execute(
                "INSERT INTO document_versions(document_id, version, title, created_at, snapshot, format_version)
                 VALUES(?1, 1, ?2, ?3, ?4, ?5)",
                params![
                    "sample-document",
                    "Backup sample",
                    1_800_000_000_000_i64,
                    &snapshot_bytes,
                    i64::from(wordcraft_engine::io::STORAGE_FORMAT_VERSION)
                ],
            )
            .expect("history snapshot is inserted");
    }

    fn write_manifest(backup_directory: &Path) -> BackupManifest {
        let database_path = backup_directory.join(DATABASE_FILE);
        let summary = verify_database(&database_path).expect("online backup passes integrity and snapshot checks");
        let manifest = BackupManifest {
            schema_version: MANIFEST_SCHEMA,
            backup_type: "daily".to_string(),
            created_at_unix_seconds: 1_800_000_000,
            created_at_utc: format_timestamp(1_800_000_000),
            database_file: DATABASE_FILE.to_string(),
            database_sha256: sha256_file(&database_path).expect("backup digest is calculated"),
            document_count: summary.documents,
            document_version_count: summary.versions,
            wordcraft_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        std::fs::write(backup_directory.join(MANIFEST_FILE), serde_json::to_vec_pretty(&manifest).expect("manifest is encoded"))
            .expect("manifest is written");
        manifest
    }

    fn create_verified_backup(scratch: &ScratchDir, label: &str) -> PathBuf {
        let source_database = scratch.path().join(format!("{label}-source.sqlite3"));
        let backup_directory = scratch.path().join(format!("daily-{label}.backup"));
        std::fs::create_dir(&backup_directory).expect("backup directory is created");
        create_sample_database(&source_database);
        create_online_backup(&source_database, &backup_directory.join(DATABASE_FILE)).expect("SQLite online backup completes");
        write_manifest(&backup_directory);
        verify_backup(&backup_directory).expect("backup verifies");
        backup_directory
    }

    fn find_quarantine_directory(parent: &Path) -> PathBuf {
        std::fs::read_dir(parent)
            .expect("scratch directory can be read")
            .map(|entry| entry.expect("scratch entry can be read").path())
            .find(|path| path.file_name().is_some_and(|name| name.to_string_lossy().starts_with(".wordcraft-prune-")))
            .expect("cleanup quarantine directory exists")
    }

    fn restored_document(path: &Path) -> Document {
        let connection = Connection::open(path).expect("restored database opens");
        let bytes = connection
            .query_row("SELECT snapshot FROM documents WHERE id = ?1", ["sample-document"], |row| row.get::<_, Vec<u8>>(0))
            .expect("restored current snapshot exists");
        let snapshot = serde_json::from_slice(&bytes).expect("restored snapshot JSON is valid");
        wordcraft_engine::io::document_from_storage_snapshot(&snapshot).expect("restored snapshot decodes")
    }

    #[test]
    fn online_backup_verifies_and_restores_document_and_embedded_assets() {
        let scratch = ScratchDir::new("roundtrip");
        let source_dir = scratch.path().join("source");
        let backup_dir = scratch.path().join("daily-2027-01-15T08-00-00-000000000Z.backup");
        let target_dir = scratch.path().join("recovery");
        std::fs::create_dir_all(&source_dir).expect("source directory is created");
        std::fs::create_dir(&backup_dir).expect("backup directory is created");

        let source_database = source_dir.join(DATABASE_FILE);
        create_sample_database(&source_database);
        create_online_backup(&source_database, &backup_dir.join(DATABASE_FILE)).expect("SQLite online backup completes");
        let backup_database = backup_dir.join(DATABASE_FILE);
        let journal_mode = Connection::open(&backup_database)
            .expect("normalized backup database opens")
            .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
            .expect("backup journal mode is readable");
        assert_eq!(journal_mode, "delete");
        assert!(SQLITE_SIDECAR_SUFFIXES.iter().all(|suffix| !sqlite_sidecar_path(&backup_database, suffix).exists()));
        let manifest = write_manifest(&backup_dir);

        let verified = verify_backup(&backup_dir).expect("SHA-256, SQLite integrity, and snapshots verify");
        assert_eq!(verified.manifest.database_sha256, manifest.database_sha256);
        assert_eq!(verified.manifest.document_count, 1);
        assert_eq!(verified.manifest.document_version_count, 1);

        let target_database = target_dir.join(DATABASE_FILE);
        restore(&backup_dir, &target_database).expect("verified backup restores to a new location");
        let restored = restored_document(&target_database);
        assert_eq!(restored.plain_text(StoryRef::Body), "Snapshot content preserved by backup.");
        assert_eq!(restored.media.get("image.png").map(|data| data.as_slice()), Some([0, 3, 42, 255].as_slice()));
        assert_eq!(restored.passthrough.get("custom.xml").map(|data| data.as_slice()), Some(b"<custom/>".as_slice()));

        let overwrite = restore(&backup_dir, &target_database);
        assert!(matches!(overwrite, Err(BackupError::Verification(message)) if message.contains("refusing to overwrite")));
        assert_eq!(restored_document(&target_database).plain_text(StoryRef::Body), "Snapshot content preserved by backup.");
    }

    #[test]
    fn restore_rejects_backup_with_a_changed_sha256() {
        let scratch = ScratchDir::new("changed-hash");
        let source_database = scratch.path().join("source.sqlite3");
        let backup_dir = scratch.path().join("daily-2027-01-15T08-00-00-000000000Z.backup");
        let target_database = scratch.path().join("recovery").join(DATABASE_FILE);
        std::fs::create_dir(&backup_dir).expect("backup directory is created");
        create_sample_database(&source_database);
        create_online_backup(&source_database, &backup_dir.join(DATABASE_FILE)).expect("SQLite online backup completes");
        write_manifest(&backup_dir);

        let mut changed_database = std::fs::OpenOptions::new()
            .append(true)
            .open(backup_dir.join(DATABASE_FILE))
            .expect("backup database opens for deliberate test corruption");
        use std::io::Write as _;
        changed_database.write_all(b"changed").expect("test corruption is written");
        drop(changed_database);

        let restore_result = restore(&backup_dir, &target_database);
        assert!(matches!(restore_result, Err(BackupError::Verification(message)) if message.contains("SHA-256 mismatch")));
        assert!(!target_database.exists());
    }

    #[test]
    fn restore_publish_rejects_a_target_created_after_the_initial_check() {
        let scratch = ScratchDir::new("restore-target-race");
        let temporary_database = scratch.path().join(".wordcraft.sqlite3.restore.tmp");
        let target_database = scratch.path().join(DATABASE_FILE);
        std::fs::write(&temporary_database, b"verified restore contents").expect("temporary database is written");
        std::fs::write(&target_database, b"concurrent target contents").expect("concurrent target is written");

        let result = publish_restore_no_clobber(&temporary_database, &target_database);

        assert!(matches!(result, Err(BackupError::Verification(message)) if message.contains("refusing to overwrite")));
        assert_eq!(std::fs::read(&target_database).expect("existing target remains readable"), b"concurrent target contents");
        assert!(!temporary_database.exists());
    }

    #[test]
    fn retention_failure_keeps_a_verified_backup_out_of_the_public_namespace() {
        let scratch = ScratchDir::new("retention-recoverable");
        let backup_directory = create_verified_backup(&scratch, "retention-recoverable");
        let verified = verify_backup(&backup_directory).expect("backup verifies before cleanup");

        let result = remove_verified_backup_with(&verified, |_| Err(std::io::Error::other("injected cleanup failure")));

        assert!(result.is_err());
        assert!(!backup_directory.exists());
        let quarantine = find_quarantine_directory(scratch.path());
        assert!(!is_backup_directory(&quarantine));
        assert!(verify_backup(&quarantine).is_ok());
    }

    #[test]
    fn retention_partial_cleanup_is_quarantined_not_exposed_as_a_backup() {
        let scratch = ScratchDir::new("retention-partial");
        let backup_directory = create_verified_backup(&scratch, "retention-partial");
        let verified = verify_backup(&backup_directory).expect("backup verifies before cleanup");

        let result = remove_verified_backup_with(&verified, |quarantine| {
            std::fs::remove_file(quarantine.join(DATABASE_FILE))?;
            Err(std::io::Error::other("injected failure after partial deletion"))
        });

        assert!(result.is_err());
        assert!(!backup_directory.exists());
        let quarantine = find_quarantine_directory(scratch.path());
        assert!(!is_backup_directory(&quarantine));
        assert!(verify_backup(&quarantine).is_err());
    }
}
