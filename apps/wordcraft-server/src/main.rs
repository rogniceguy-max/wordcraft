use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tower_http::services::ServeDir;
use wordcraft_server::{Store, api_router};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("wordcraft-server: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let bind = std::env::var("WORDCRAFT_BIND").unwrap_or_else(|_| "127.0.0.1:8787".into());
    let address = bind.parse::<SocketAddr>()?;
    let data_dir = std::env::var_os("WORDCRAFT_DATA_DIR").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("data"));
    let web_dir = std::env::var_os("WORDCRAFT_WEB_DIR").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("dist/web"));
    std::fs::create_dir_all(&data_dir)?;
    let database = data_dir.join("wordcraft.sqlite3");
    let store = Arc::new(Store::open(&database).map_err(std::io::Error::other)?);
    let app = api_router(store).fallback_service(ServeDir::new(web_dir.clone()).append_index_html_on_directories(true));
    let listener = tokio::net::TcpListener::bind(address).await?;

    eprintln!("WordCraft server listening on http://{address}");
    eprintln!("Document database: {}", database.display());
    eprintln!("Static web directory: {}", web_dir.display());
    if address.ip().is_unspecified() {
        eprintln!("WARNING: server is bound to every network interface; use only behind a trusted network boundary and HTTPS reverse proxy");
    }

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("could not listen for Ctrl-C: {error}");
            }
        })
        .await?;
    Ok(())
}
