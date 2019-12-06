use futures::{io::AsyncWrite, lock::Mutex};
use indexmap::map::{Entry as MapEntry, IndexMap};
use polyfuse::{
    reply::{ReplyAttr, ReplyEntry},
    Context, DirEntry, FileAttr, Forget, Operation,
};
use std::{
    ffi::{OsStr, OsString},
    fmt, io,
};

pub const ROOT_INO: u64 = 1;

#[polyfuse::async_trait]
pub trait Node: Send + Sync + 'static {
    fn get_attr(&self) -> FileAttr;
    fn set_attr(&self, attr: FileAttr);

    async fn read(&self, _offset: u64, _buf: &mut [u8]) -> io::Result<usize> {
        unreachable!("wrong method call")
    }
}

// ==== INodeTable ====

struct INode {
    attr: FileAttr,
    node: Box<dyn Node>,
    kind: INodeKind,
}

impl fmt::Debug for INode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("INode")
            .field("attr", &self.attr)
            .field("kind", &self.kind)
            .field("node", &"<node>")
            .finish()
    }
}

#[derive(Debug)]
enum INodeKind {
    File,
    Dir { children: IndexMap<OsString, u64> },
}

impl INode {
    fn new<T: Node>(attr: FileAttr, node: T) -> Self {
        let kind = match attr.mode() & libc::S_IFMT {
            libc::S_IFDIR => INodeKind::Dir {
                children: IndexMap::new(),
            },
            _ => INodeKind::File,
        };

        Self {
            attr,
            node: Box::new(node),
            kind,
        }
    }
}

#[derive(Debug)]
struct INodeTable {
    inodes: IndexMap<u64, INode>,
    next_id: u64,
}

impl INodeTable {
    fn lookup(&self, parent: u64, name: &OsStr) -> Option<&dyn Node> {
        match self.inodes.get(&parent)?.kind {
            INodeKind::Dir { ref children, .. } => {
                let child = children.get(name)?;
                self.get(*child)
            }
            _ => None,
        }
    }

    fn forget(&self, _forgets: &[Forget]) {
        ()
    }

    fn get(&self, ino: u64) -> Option<&dyn Node> {
        Some(&*self.inodes.get(&ino)?.node)
    }

    fn insert<T: Node>(&mut self, parent: u64, name: OsString, node: T) -> Result<(), i32> {
        match self.inodes.get_mut(&parent).ok_or(libc::ENOENT)? {
            INode {
                kind: INodeKind::Dir { children, .. },
                ..
            } => match children.entry(name) {
                MapEntry::Occupied(..) => return Err(libc::EEXIST),
                MapEntry::Vacant(entry) => {
                    let mut attr = node.get_attr();
                    attr.set_ino(self.next_id);
                    self.next_id += 1;
                    node.set_attr(attr);

                    entry.insert(attr.ino());
                    self.inodes.insert(attr.ino(), INode::new(attr, node));

                    Ok(())
                }
            },
            _ => return Err(libc::ENOTDIR),
        }
    }

    fn read_dir(&self, ino: u64, offset: u64, bufsize: usize) -> Result<Vec<DirEntry>, i32> {
        match self.inodes.get(&ino).ok_or(libc::ENOENT)? {
            INode {
                kind: INodeKind::Dir { children, .. },
                ..
            } => {
                let mut entries = vec![];
                let mut total_len = 0;
                for (i, (name, child)) in children.iter().enumerate().skip(offset as usize) {
                    if let Some(child) = self.inodes.get(child) {
                        let ino = child.attr.ino();
                        let mut entry = DirEntry::new(name, ino, (i + 1) as u64);
                        entry.set_typ(child.attr.mode() & libc::S_IFMT >> 12);

                        total_len += entry.as_ref().len();
                        if total_len > bufsize {
                            break;
                        }
                        entries.push(entry);
                    }
                }
                Ok(entries)
            }
            _ => return Err(libc::ENOTDIR),
        }
    }
}

// ==== NodeFS ====

#[derive(Debug)]
pub struct NodeFS {
    inodes: Mutex<INodeTable>,
}

impl NodeFS {
    pub fn new<T: Node>(root: T) -> Self {
        let mut attr = root.get_attr();
        attr.set_ino(1);
        root.set_attr(attr);

        let mut inodes = IndexMap::new();
        inodes.insert(ROOT_INO, INode::new(attr, root));

        Self {
            inodes: Mutex::new(INodeTable {
                inodes,
                next_id: ROOT_INO + 1, // The first inode is root.
            }),
        }
    }

    pub async fn insert<T: Node>(&self, parent: u64, name: OsString, node: T) -> Result<(), i32> {
        let mut inodes = self.inodes.lock().await;
        inodes.insert(parent, name, node)
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn reply<W: ?Sized, T>(
        &self,
        cx: &mut Context<'_, W>,
        op: Operation<'_, T>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match op {
            Operation::Lookup(op) => {
                let inodes = self.inodes.lock().await;
                match inodes.lookup(op.parent(), op.name()) {
                    Some(inode) => {
                        let mut reply = ReplyEntry::new(inode.get_attr());
                        reply.entry_valid(u64::max_value(), 0);
                        reply.attr_valid(u64::max_value(), 0);
                        op.reply(cx, reply).await?;
                    }
                    None => cx.reply_err(libc::ENOENT).await?,
                }
            }

            Operation::Forget(forgets) => {
                let inodes = self.inodes.lock().await;
                inodes.forget(forgets);
            }

            Operation::Getattr(op) => {
                let inodes = self.inodes.lock().await;
                match inodes.get(op.ino()) {
                    Some(inode) => {
                        let mut reply = ReplyAttr::new(inode.get_attr());
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
                        Some(file) => {
                            let mut buf = vec![0u8; op.size() as usize];
                            let len = file.read(op.offset(), &mut buf[..]).await?;
                            op.reply(cx, &buf[..len]).await?;
                        }
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
