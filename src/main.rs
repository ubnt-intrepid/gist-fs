use gist_fs::{client::Client, fs::GistFs};
use pico_args::Arguments;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let mut args = Arguments::from_env();

    let gist_id = args.value_from_str("--gist-id")?;

    let mountpoint: PathBuf = args
        .free_from_str()?
        .ok_or_else(|| anyhow::anyhow!("missing mountpoint"))?;
    anyhow::ensure!(mountpoint.is_dir(), "the mountpoint must be a directory");

    let token = std::env::var("GITHUB_TOKEN").ok();
    let client = Client::new(token);

    let fs = GistFs::new(client, gist_id).await?;
    tracing::trace!("fs={:?}", fs);

    polyfuse_tokio::mount(
        fs, //
        mountpoint,
        &["-o".as_ref(), "fsname=gistfs".as_ref()],
    )
    .await?;

    Ok(())
}
