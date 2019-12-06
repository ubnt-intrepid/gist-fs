use crate::{
    gist::GistClient,
    node::{Node, NodeFS, ROOT_INO},
};
use crossbeam::atomic::AtomicCell;
use futures::io::AsyncWrite;
use polyfuse::{Context, FileAttr, Filesystem, Operation};
use std::io;

pub struct GistFs {
    nodes: NodeFS,
    client: GistClient,
}

impl GistFs {
    pub fn new(client: GistClient) -> Self {
        Self {
            nodes: NodeFS::new(RootNode::new()),
            client,
        }
    }

    // TODO:
    // * invalidate the old files
    // * update directory entries
    pub async fn fetch_gist(&self) -> anyhow::Result<()> {
        let gist = self.client.fetch().await?;
        for (filename, file) in gist.files {
            self.nodes
                .insert(
                    ROOT_INO,
                    filename.into(),
                    GistFileNode {
                        attr: {
                            let mut attr = FileAttr::default();
                            attr.set_nlink(1);
                            attr.set_mode(libc::S_IFREG | 0o444);
                            attr.set_size(file.size as u64);
                            attr.set_uid(unsafe { libc::getuid() });
                            attr.set_gid(unsafe { libc::getuid() });
                            AtomicCell::new(attr)
                        },
                        content: file.content.into(),
                    },
                )
                .await
                .map_err(std::io::Error::from_raw_os_error)?;
        }

        Ok(())
    }
}

#[polyfuse::async_trait]
impl<T> Filesystem<T> for GistFs {
    async fn call<W: ?Sized>(&self, cx: &mut Context<'_, W>, op: Operation<'_, T>) -> io::Result<()>
    where
        T: Send + 'async_trait,
        W: AsyncWrite + Unpin + Send,
    {
        self.nodes.reply(cx, op).await
    }
}

// ==== RootNode ====

struct RootNode {
    attr: AtomicCell<FileAttr>,
}

impl RootNode {
    fn new() -> Self {
        Self {
            attr: {
                let mut attr = FileAttr::default();
                attr.set_ino(ROOT_INO);
                attr.set_mode(libc::S_IFDIR | 0o555);
                attr.set_uid(unsafe { libc::getuid() });
                attr.set_gid(unsafe { libc::getgid() });
                attr.set_nlink(2);
                AtomicCell::new(attr)
            },
        }
    }
}

impl Node for RootNode {
    fn get_attr(&self) -> FileAttr {
        self.attr.load()
    }

    fn set_attr(&self, attr: FileAttr) {
        self.attr.store(attr)
    }
}

// ==== FileNode ====

struct GistFileNode {
    attr: AtomicCell<FileAttr>,
    content: Vec<u8>,
}

#[polyfuse::async_trait]
impl Node for GistFileNode {
    fn get_attr(&self) -> FileAttr {
        self.attr.load()
    }

    fn set_attr(&self, attr: FileAttr) {
        self.attr.store(attr);
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let offset = offset as usize;
        if offset > self.content.len() {
            return Ok(0);
        }

        let content = &self.content[offset..];
        let len = std::cmp::min(content.len(), buf.len());
        buf[..len].copy_from_slice(&content[..len]);

        Ok(len)
    }
}
