use gist_fs::{fs::GistFs, gist::GistClient};
use pico_args::Arguments;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mut args = Arguments::from_env();

    let gist_id = args.value_from_str("--gist-id")?;
    let github_token = std::env::var("GITHUB_TOKEN").ok();
    let client = GistClient::new(gist_id, github_token);

    let mountpoint: PathBuf = args
        .free_from_str()?
        .ok_or_else(|| anyhow::anyhow!("missing mountpoint"))?;
    anyhow::ensure!(mountpoint.is_dir(), "the mountpoint must be a directory");

    let fs = GistFs::new(client);
    fs.fetch_gist().await?;

    polyfuse_tokio::mount(
        fs,//
        mountpoint,
        &["-o".as_ref(), "fsname=gistfs".as_ref()],
    )
    .await?;

    Ok(())
}
