use crate::{
    gist::GistClient,
    inode::{INodeTable, Node, ROOT_INO},
};
use crossbeam::atomic::AtomicCell;
use futures::{io::AsyncWrite, lock::Mutex};
use polyfuse::{
    reply::{ReplyAttr, ReplyEntry},
    Context, FileAttr, Filesystem, Operation,
};
use std::io;

pub struct GistFs {
    client: GistClient,
    inodes: Mutex<INodeTable>,
}

impl GistFs {
    pub fn new(client: GistClient) -> Self {
        Self {
            client,
            inodes: Mutex::new(INodeTable::new(RootNode::new())),
        }
    }

    // TODO:
    // * invalidate the old files
    // * update directory entries
    pub async fn fetch_gist(&self) -> anyhow::Result<()> {
        let gist = self.client.fetch().await?;

        let mut inodes = self.inodes.lock().await;
        for (filename, file) in gist.files {
            inodes
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
                .map_err(std::io::Error::from_raw_os_error)?;
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
            Operation::Lookup(op) => {
                let inodes = self.inodes.lock().await;
                match inodes.lookup(op.parent(), op.name()) {
                    Some(inode) => {
                        let mut reply = ReplyEntry::new(inode.attr());
                        reply.entry_valid(u64::max_value(), 0);
                        reply.attr_valid(u64::max_value(), 0);
                        op.reply(cx, reply).await?;
                    }
                    None => cx.reply_err(libc::ENOENT).await?,
                }
            }

            Operation::Forget(forgets) => self.inodes.lock().await.forget(forgets),

            Operation::Getattr(op) => {
                let inodes = self.inodes.lock().await;
                match inodes.get(op.ino()) {
                    Some(inode) => {
                        let mut reply = ReplyAttr::new(inode.attr());
                        reply.attr_valid(u64::max_value(), 0);
                        op.reply(cx, reply).await?;
                    }
                    None => cx.reply_err(libc::ENOENT).await?,
                }
            }

            Operation::Read(op) => match op.ino() {
                1 => cx.reply_err(libc::EISDIR).await?,
                ino => {
                    let inodes = self.inodes.lock().await;
                    match inodes.get(ino) {
                        Some(inode) => match inode.downcast_ref::<GistFileNode>() {
                            Some(file) => {
                                let content = file.read(op.offset() as usize, op.size() as usize);
                                op.reply(cx, content).await?;
                            }
                            None => cx.reply_err(libc::EIO).await?,
                        },
                        None => cx.reply_err(libc::ENOENT).await?,
                    }
                }
            },

            Operation::Readdir(op) => {
                let inodes = self.inodes.lock().await;
                match inodes.read_dir(ROOT_INO, op.offset(), op.size() as usize) {
                    Ok(dirents) => {
                        let dirents: Vec<&[u8]> =
                            dirents.iter().map(|entry| entry.as_ref()).collect();
                        op.reply_vectored(cx, &dirents[..]).await?;
                    }
                    Err(errno) => cx.reply_err(errno).await?,
                }
            }

            _ => (),
        }

        Ok(())
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
    fn attr(&self) -> FileAttr {
        self.attr.load()
    }

    fn store_attr(&self, attr: FileAttr) {
        self.attr.store(attr)
    }
}

struct GistFileNode {
    attr: AtomicCell<FileAttr>,
    content: Vec<u8>,
}

impl GistFileNode {
    fn read(&self, offset: usize, len: usize) -> &[u8] {
        if offset > self.content.len() {
            return &[];
        }

        let content = &self.content[offset..];
        &content[..std::cmp::min(content.len(), len)]
    }
}

impl Node for GistFileNode {
    fn attr(&self) -> FileAttr {
        self.attr.load()
    }

    fn store_attr(&self, attr: FileAttr) {
        self.attr.store(attr);
    }
}
