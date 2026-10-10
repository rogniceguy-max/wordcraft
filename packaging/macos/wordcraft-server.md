# WordCraft server on a headless Mac

The server stores the current document, embedded media, source-format parts, and immutable
document versions in one SQLite database. It serves the Trunk-built web editor and the `/api`
document API from the same origin.

## Build and install

Build natively on the Mac mini so the executable targets Apple Silicon ARM64:

```sh
cd /path/to/WordCraftWeb
cargo build --release -p wordcraft-server --bin wordcraft-server --bin wordcraft-backup
(cd apps/wordcraft-web && trunk build --release)
```

Install `target/release/wordcraft-server` and the generated `dist/web` files under a stable
directory readable by a dedicated, unprivileged service account. The LaunchDaemon template is
`packaging/macos/com.storytold.wordcraft-server.plist`; replace its paths and service-account
placeholder before installation. The service account does not need an interactive desktop login.

Create the data, web, log, and backup directories with ownership restricted to that service
account. The document database defaults to `WORDCRAFT_DATA_DIR/wordcraft.sqlite3`. The browser
assets default to `WORDCRAFT_WEB_DIR`; the plist points both at the stable install directory.

```sh
sudo install -o root -g wheel -m 644 \
  packaging/macos/com.storytold.wordcraft-server.plist \
  /Library/LaunchDaemons/com.storytold.wordcraft-server.plist
sudo launchctl bootstrap system /Library/LaunchDaemons/com.storytold.wordcraft-server.plist
sudo launchctl print system/com.storytold.wordcraft-server
curl http://127.0.0.1:8787/api/health
```

`RunAtLoad` starts the service without a GUI session. `KeepAlive` asks launchd to restart it after
an unexpected exit. Logs go to the service account's `Library/Logs` directory. The server defaults
to `127.0.0.1:8787`; it has no built-in TLS or user authentication. To allow other computers on the
LAN to connect, put a reverse proxy on the Mac in front of the loopback service and configure HTTPS
and the intended network boundary there. Do not expose the plain HTTP listener directly to an
untrusted network.

The web app and API share an origin when served by this binary, so the client needs no permissive
CORS policy. Use HTTPS at the reverse proxy for browser features that require a secure context,
including WebGPU on non-localhost addresses; the editor retains its WebGL2 fallback.

## Backup service

Install `wordcraft-backup` and `packaging/macos/com.storytold.wordcraft-backup.plist` with the same
service account and data directory. See [wordcraft-backup.md](wordcraft-backup.md) for backup,
verification, restore, and retention commands. The backup daemon runs at 03:00 local time.

## Acceptance still required on the Mac

This development environment is Windows and is not connected to the target Mac mini. The plist
templates and ARM64 build instructions are supplied, but neither Apple Silicon compilation nor a
headless reboot test has been run here. On the Mac, verify the process survives a reboot before a
user opens a desktop session, check both launchd logs, create and verify a backup, and restore that
backup into a new test directory before using the service for shared documents.
