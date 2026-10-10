# WordCraft server backups on macOS

WordCraft stores each document snapshot, version snapshot, media object, and passthrough object in
the single SQLite database `WORDCRAFT_DATA_DIR/wordcraft.sqlite3`. The `wordcraft-backup` utility
uses SQLite's online backup API, then writes a manifest with a SHA-256 digest and snapshot counts.
Verification checks the file digest, SQLite `integrity_check`, foreign keys, and every snapshot in
both the current-document and version-history tables.

## Commands

Build the standalone backup binary with the project's Rust toolchain:

```sh
cargo build --release -p wordcraft-server --bin wordcraft-backup
```

The binary is at `target/release/wordcraft-backup`. Install it at the path used by the plist, for
example:

```sh
sudo install -m 755 target/release/wordcraft-backup /usr/local/bin/wordcraft-backup
```

The data directory defaults to `data`, matching the server. Set `WORDCRAFT_DATA_DIR` to the
server's actual data directory. Backups default to `WORDCRAFT_DATA_DIR/backups`; set
`WORDCRAFT_BACKUP_DIR` to a separate destination if desired. Keep the backup directory on a
different volume or storage service when protection against disk failure matters, and restrict it
to the WordCraft service account.

```sh
wordcraft-backup daily [BACKUP_DIR]
wordcraft-backup list [BACKUP_DIR]
wordcraft-backup verify BACKUP_DIR
wordcraft-backup restore BACKUP_DIR --target-dir /path/to/empty/recovery-directory
wordcraft-backup restore BACKUP_DIR --target-db /path/to/recovery.sqlite3
```

`daily` first creates a verified SQLite online backup and its manifest, then applies retention.
Retention keeps the 14 newest verified daily backups and one newest backup in each of the next 8
older weekly buckets. Corrupt or unrecognized backup directories are left in place for inspection.
The utility never removes the last verified backup. A failed cleanup is written to standard error
and makes the command exit unsuccessfully; launchd sends that output to the configured error log.
`restore` verifies the source first and refuses to overwrite an existing target database. It writes
to a new, separate location; it does not replace the server's live database.

## Install the LaunchDaemon

The supplied plist is a template. Replace every `CHANGE_ME` value and change the binary path if
needed before installing it:

`packaging/macos/com.storytold.wordcraft-backup.plist`

Use the same unprivileged macOS service account and data directory as the server. Ensure the backup
and `Library/Logs` directories exist and are writable by that account. The backup destination
should be separate from the live database directory when practical. Then install the plist with
the required LaunchDaemon ownership and mode:

```sh
sudo install -o root -g wheel -m 644 \
  packaging/macos/com.storytold.wordcraft-backup.plist \
  /Library/LaunchDaemons/com.storytold.wordcraft-backup.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.storytold.wordcraft-backup.plist
sudo launchctl print system/com.storytold.wordcraft-backup
```

`StartCalendarInterval` schedules the daily run at **03:00 local time**. To exercise the installed
job without waiting for the next schedule, run:

```sh
sudo launchctl kickstart -k system/com.storytold.wordcraft-backup
```

Check `WordCraftBackup.stdout.log` and `WordCraftBackup.stderr.log` under the service account's
`Library/Logs` directory, then run `wordcraft-backup list` and `wordcraft-backup verify` on the new
backup. A real macOS reboot acceptance has **not** been performed; after deployment, reboot the Mac
and confirm the daemon is loaded and the next scheduled run succeeds.

## Network exposure

The server defaults to binding only `127.0.0.1:8787`. Keep it loopback-only for local use. If it
must be reached from another device, place it behind a trusted reverse proxy that provides HTTPS
and access control; do not expose the plain HTTP server directly to an untrusted network. Changing
`WORDCRAFT_BIND` to a non-loopback address does not add TLS or authentication to the server.
