//! Gist as a filesystem.

#![allow(dead_code)]

use futures::{io::AsyncWrite, lock::Mutex};
use gist_client::{Client, ETag, Gist};
use node_table::{Node, NodeTable};
use polyfuse::{
    op,
    reply::{ReplyAttr, ReplyEntry, ReplyOpendir},
    Context, FileAttr, Filesystem, Operation,
};
use std::{collections::HashMap, io, sync::Arc};

pub struct GistFs {
    client: Client,
    gist_id: String,
    node_table: NodeTable,
    files: GistFiles,
}

impl GistFs {
    pub fn new(client: Client, gist_id: String) -> Self {
        let node_table = NodeTable::new({
            let mut root_attr = FileAttr::default();
            root_attr.set_mode(libc::S_IFDIR | 0o555);
            root_attr.set_uid(unsafe { libc::getuid() });
            root_attr.set_gid(unsafe { libc::getgid() });
            root_attr.set_nlink(2);
            root_attr
        });

        Self {
            client,
            gist_id,
            node_table,
            files: GistFiles::default(),
        }
    }

    // TODO:
    // * invalidate the old files
    pub async fn fetch_gist(&self) -> anyhow::Result<()> {
        tracing::debug!("fetch Gist content");
        let etag = self.files.etag.lock().await.clone();
        let response = self.client.fetch_gist(&self.gist_id, etag.as_ref()).await?;

        if let Some((gist, etag)) = response {
            tracing::debug!("update Gist content: gist={:?}, etag={:?}", gist, etag);
            self.files.update(gist, etag, &self.node_table).await?;
        } else {
            tracing::debug!("use cached Gist content");
        }

        Ok(())
    }
}

#[polyfuse::async_trait]
impl<T> Filesystem<T> for GistFs {
    #[allow(clippy::cognitive_complexity)]
    async fn call<W: ?Sized>(&self, cx: &mut Context<'_, W>, op: Operation<'_, T>) -> io::Result<()>
    where
        T: Send + 'async_trait,
        W: AsyncWrite + Unpin + Send,
    {
        match op {
            Operation::Lookup(op) => match self.node_table.lookup(op.parent(), op.name()).await {
                Some(node) => {
                    let mut reply = ReplyEntry::new(node.attr());
                    reply.entry_valid(0, 0);
                    reply.attr_valid(0, 0);
                    op.reply(cx, reply).await?
                }
                None => cx.reply_err(libc::ENOENT).await?,
            },

            Operation::Forget(forgets) => self.node_table.forget(forgets).await,

            Operation::Getattr(op) => match self.node_table.get(op.ino()).await {
                Some(node) => {
                    let mut reply = ReplyAttr::new(node.attr());
                    reply.attr_valid(0, 0);
                    op.reply(cx, reply).await?
                }
                None => cx.reply_err(libc::ENOENT).await?,
            },

            Operation::Opendir(op) => match op.ino() {
                1 => match self.fetch_gist().await {
                    Ok(()) => {
                        let mut reply = ReplyOpendir::new(0);
                        reply.cache_dir(false);
                        op.reply(cx, reply).await?;
                    }
                    Err(err) => {
                        tracing::error!("fetch failed: {}", err);
                        cx.reply_err(libc::EIO).await?;
                    }
                },
                _ => cx.reply_err(libc::ENOTDIR).await?,
            },

            Operation::Readdir(op) => self.node_table.root().readdir(cx, op).await?,

            Operation::Read(op) => match self.files.get(op.ino()).await {
                Some(file) => file.read(cx, op).await?,
                None => cx.reply_err(libc::ENOENT).await?,
            },

            _ => (),
        }

        Ok(())
    }
}

// ==== Files ====

#[derive(Default)]
struct GistFiles {
    etag: Mutex<Option<ETag>>,
    files: Mutex<HashMap<u64, Arc<GistFileNode>>>,
}

impl GistFiles {
    async fn get(&self, ino: u64) -> Option<Arc<GistFileNode>> {
        let files = self.files.lock().await;
        files.get(&ino).cloned()
    }

    async fn update(
        &self,
        gist: Gist,
        etag: Option<ETag>,
        node_table: &NodeTable,
    ) -> anyhow::Result<()> {
        let old_files = {
            let mut files = self.files.lock().await;

            let mut new_files = HashMap::with_capacity(files.len());
            for (filename, gist_file) in gist.files {
                let ino = files
                    .iter()
                    .find(|(_, file)| file.filename == filename)
                    .map(|(ino, _)| *ino);
                match ino {
                    Some(ino) => {
                        tracing::debug!("update an exist file: filename={:?}", gist_file.filename);
                        let file = files.remove(&ino).unwrap();
                        file.update_content(gist_file.size, gist_file.content).await;
                        new_files.insert(ino, file);
                    }
                    None => {
                        tracing::debug!("new file: filename={:?}", gist_file.filename);
                        let mut attr = FileAttr::default();
                        attr.set_nlink(1);
                        attr.set_mode(libc::S_IFREG | 0o444);
                        attr.set_size(gist_file.size as u64);
                        attr.set_uid(unsafe { libc::getuid() });
                        attr.set_gid(unsafe { libc::getgid() });

                        let node = node_table
                            .root()
                            .new_child(filename.clone().into(), attr)
                            .await
                            .map_err(std::io::Error::from_raw_os_error)?;

                        new_files.insert(
                            node.attr().ino(),
                            Arc::new(GistFileNode {
                                node,
                                filename,
                                content: Mutex::new(gist_file.content.into()),
                            }),
                        );
                    }
                }
            }

            std::mem::replace(&mut *files, new_files)
        };

        for (ino, file) in old_files {
            tracing::debug!("remove a file: ino={}, filename={:?}", ino, file.filename);
            file.node.remove().await;
        }

        if let Some(etag) = etag {
            self.etag.lock().await.replace(etag);
        }

        Ok(())
    }
}

// ==== FileNode ====

#[derive(Debug)]
struct GistFileNode {
    node: Node,
    filename: String,
    content: Mutex<Vec<u8>>,
}

impl GistFileNode {
    async fn update_content(&self, size: u64, content: impl Into<Vec<u8>>) {
        let mut attr = self.node.attr();
        attr.set_size(size);
        self.node.set_attr(attr);

        *self.content.lock().await = content.into();
    }

    async fn read<W: ?Sized>(&self, cx: &mut Context<'_, W>, op: op::Read<'_>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let content = self.content.lock().await;

        let offset = op.offset() as usize;
        if offset > content.len() {
            return op.reply(cx, &[]).await;
        }

        let content = &content[offset..];
        let len = std::cmp::min(content.len(), op.size() as usize);
        op.reply(cx, &content[..len]).await?;

        Ok(())
    }
}
