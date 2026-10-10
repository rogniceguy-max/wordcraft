# Hosting WordCraft for the web

`wordcraft-web-<version>.zip` (from the GitHub release, or `packaging/web/package.sh`) holds a
static site in `wordcraft-web-<version>/`:

| File | What it is |
|---|---|
| `index.html` | The page. It loads everything through relative URLs. |
| `wordcraft-web-<hash>.js` | wasm-bindgen glue (generated, ES module) |
| `wordcraft-web-<hash>_bg.wasm` | The app, about 13 MB, or 5 MB with compression |
| `_headers`, `.htaccess` | Sample header rules for Netlify/Cloudflare Pages and Apache |

The release zip contains only the static browser assets. It can still be hosted as a stand-alone
editor, but it does not include persistence or collaboration. For server-backed shared documents,
build and run the `wordcraft-server` companion described below.

## Shared-document server

`wordcraft-server` serves both these assets and the same-origin `/api` used by the web editor. It
stores versioned WordCraft document snapshots, including embedded media and preserved source parts,
in SQLite. The browser exposes separate actions for saving to the server, saving as a new server
document, and downloading exported copies. A stale save is rejected with a version conflict, and
local edits remain open so the user can reload the newer server version or save a separate copy.
Unsaved edits are also written to browser `localStorage` after 500 ms without a new edit, for
recovery after a reload. The recovery snapshot is limited to 2 MiB of serialized JSON to leave room under common per-origin
storage quotas; oversized drafts or blocked/quota-full storage are reported in the app, and the
browser warns before leaving when edits remain unsaved. Download a copy before leaving if the app
reports that local recovery is unavailable.

This is the persistent-document milestone; it does **not** yet provide WebSocket-based real-time
co-editing. Two clients editing concurrently will be protected from silent overwrite, but they will
not see one another's keystrokes until operation-based collaboration is implemented.

```sh
cargo build --release -p wordcraft-server --bin wordcraft-server
(cd apps/wordcraft-web && trunk build --release)
WORDCRAFT_BIND=127.0.0.1:8787 \
WORDCRAFT_DATA_DIR=./data \
WORDCRAFT_WEB_DIR=./dist/web \
./target/release/wordcraft-server
```

Open `http://127.0.0.1:8787/`. For macOS LaunchDaemon, HTTPS reverse proxy, and service-account
setup, see [the headless Mac deployment guide](../macos/wordcraft-server.md). The server defaults to
loopback and has no built-in TLS or user login; keep it behind a trusted reverse proxy if other
devices need access.

## Any path works

All URLs in `index.html` are relative (`public_url = "./"` in `apps/wordcraft-web/Trunk.toml`),
so the site works at a domain root (`https://example.com/`), under a prefix
(`https://example.com/tools/wordcraft/`) and from a CDN bucket. The asset names carry a content
hash, so they can be cached forever. Only `index.html` needs revalidation.

## Required server settings

- **MIME type:** serve `.wasm` as `application/wasm`. Browsers refuse to stream-compile it under
  any other type, and the app then loads slowly or not at all. Serve `.js` as `text/javascript`.
  Most hosts already do both. For nginx, check that `mime.types` has `application/wasm wasm;`.
- **Compression:** turn on gzip or Brotli for `.wasm`, `.js` and `.html`. That takes the
  download from about 13 MB to about 5 MB. You can also precompress (`brotli -k *.wasm`) and let
  the server send `Content-Encoding: br`.
- **Caching:** `Cache-Control: public, max-age=31536000, immutable` on the hashed `.wasm` and
  `.js` files, and `no-cache` on `index.html`.
- **HTTPS:** WebGPU (and the clipboard) only work in a secure context, which means `https://`
  or `http://localhost`. Over plain HTTP elsewhere, the app falls back to WebGL2.
- **No special isolation headers:** WordCraft doesn't use `SharedArrayBuffer`, so it doesn't
  need `Cross-Origin-Opener-Policy` or `Cross-Origin-Embedder-Policy`. If your site already sends
  COEP `require-corp`, also send `Cross-Origin-Resource-Policy: same-origin` (or `cross-origin`
  when the files live on a CDN) on the app's files.

nginx example:

```nginx
location /wordcraft/ {
    types { application/wasm wasm; text/javascript js; text/html html; }
    gzip on;
    gzip_types application/wasm text/javascript text/html;
    location ~* \.(wasm|js)$ { add_header Cache-Control "public, max-age=31536000, immutable"; }
    location ~* index\.html$ { add_header Cache-Control "no-cache"; }
}
```

Local test: `python3 -m http.server 8765` inside the folder, then open http://localhost:8765/.

## Embedding in a page (iframe)

```html
<iframe
  src="https://example.com/wordcraft/"
  title="WordCraft image editor"
  style="width: 100%; height: 720px; border: 0;"
  allow="fullscreen; clipboard-read; clipboard-write"
  allowfullscreen>
</iframe>
```

- The app fills the iframe and follows its size, so size the iframe and not the app.
- Keyboard shortcuts go to the iframe after the user clicks into it, as with any embedded app.
- **Cross-origin embeds** work. Preferences are kept in the iframe's `localStorage`. Browsers
  that partition or block third-party storage may forget them between visits, and the app
  then starts with defaults.
- **Sandboxed iframes** need at least
  `sandbox="allow-scripts allow-same-origin allow-downloads allow-popups"`. Without
  `allow-same-origin` there's no local preference storage. Without `allow-downloads`, DOCX/PDF
  exports are blocked. Server saves use the hosting origin's `/api` endpoint.
- Don't send `X-Frame-Options: DENY` or a `frame-ancestors` CSP that excludes the embedding page.

## Renderer selection and fallback flags

WordCraft renders with wgpu. It uses **WebGPU** when the browser has it and falls back to
**WebGL2** on its own. URL query flags override this, and they work on the iframe `src` too:

| Flag | Effect |
|---|---|
| *(none)* | WebGPU if available, otherwise WebGL2 |
| `?webgl` | Force the WebGL2 backend (useful when a WebGPU driver misbehaves) |
| `?cpu` | Force the CPU canvas path (slowest, most compatible) |

For example: `<iframe src="https://example.com/wordcraft/?webgl" ...>`.

A browser with neither WebGPU nor WebGL2 gets a message in place of the app.
