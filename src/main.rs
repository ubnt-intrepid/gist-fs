use gist_fs::{fs::GistFs, gist::GistClient};
use pico_args::Arguments;
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = Arguments::from_env();

    let log_level = args
        .opt_value_from_str("--log-level")?
        .unwrap_or(Level::DEBUG);
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let gist_id = args.value_from_str("--gist-id")?;
    let github_token = std::env::var("GITHUB_TOKEN").ok();
    let client = GistClient::new(gist_id, github_token);

    let mountpoint: PathBuf = args
        .free_from_str()?
        .ok_or_else(|| anyhow::anyhow!("missing mountpoint"))?;
    anyhow::ensure!(mountpoint.is_dir(), "the mountpoint must be a directory");

    let fs = GistFs::new(client);
    fs.fetch_gist().await?;

    polyfuse_tokio::mount(fs, mountpoint, &[]).await?;

    Ok(())
}
