use crossbeam::atomic::AtomicCell;
use futures::{io::AsyncWrite, lock::Mutex};
use indexmap::map::{Entry as MapEntry, IndexMap};
use polyfuse::{op, Context, DirEntry, FileAttr, Forget};
use std::{
    ffi::{OsStr, OsString},
    io,
    sync::{Arc, Weak},
};

/// A database that manages the hierarchical structure of nodes in a filesystem.
#[derive(Debug)]
pub struct NodeTable {
    inner: Arc<NodeTableInner>,
    root: Weak<NodeInner>,
}

#[derive(Debug)]
struct NodeTableInner {
    nodes: Mutex<IndexMap<u64, Arc<NodeInner>>>,
    next_ino: AtomicCell<u64>,
}

/// A handle of the node linked to `NodeTable`.
#[derive(Debug)]
pub struct Node {
    nodeid: u64,
    inner: Weak<NodeInner>,
}

#[derive(Debug)]
struct NodeInner {
    attr: AtomicCell<FileAttr>,
    kind: NodeKind,
    nlookup: AtomicCell<u64>,
}

#[derive(Debug)]
enum NodeKind {
    File,
    Dir(Mutex<DirNode>),
}

#[derive(Debug)]
struct DirNode {
    children: IndexMap<OsString, (u64, DirEntry)>,
    dirents: [DirEntry; 2],
}

impl DirNode {
    fn entries<'a>(&'a self) -> impl Iterator<Item = &'a DirEntry> + 'a {
        self.dirents
            .iter()
            .chain(self.children.values().map(|&(_, ref entry)| entry))
    }

    async fn reply_readdir<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Readdir<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let offset = op.offset() as usize;
        let mut total_len = 0;
        let bufsize = op.size() as usize;

        let entries: Vec<&[u8]> = self
            .entries()
            .skip(offset)
            .map(|entry| entry.as_ref())
            .take_while(|entry| {
                total_len += entry.len();
                total_len <= bufsize
            })
            .collect();
        op.reply_vectored(cx, &entries[..]).await
    }
}

impl NodeTable {
    /// Create a new `NodeTable`.
    ///
    /// The constructor takes an attribute of the root directory and uses its value.
    /// At this time, some properties, such as `ino` are replaced with the appropriate
    /// values.
    pub fn new(mut root_attr: FileAttr) -> Self {
        root_attr.set_ino(1);
        root_attr.set_mode((root_attr.mode() & !libc::S_IFMT) | libc::S_IFDIR);
        root_attr.set_nlink(2);

        let root = Arc::new(NodeInner {
            attr: AtomicCell::new(root_attr),
            kind: NodeKind::Dir(Mutex::new(DirNode {
                children: IndexMap::new(),
                dirents: [DirEntry::dir(".", 1, 1), DirEntry::dir("..", 1, 2)],
            })),
            nlookup: AtomicCell::new(1),
        });
        let root_ptr = Arc::downgrade(&root);

        let mut nodes = IndexMap::new();
        nodes.insert(1, root);

        Self {
            inner: Arc::new(NodeTableInner {
                nodes: Mutex::new(nodes),
                next_ino: AtomicCell::new(2), // ino=1 is used by the root inode.
            }),
            root: root_ptr,
        }
    }

    /// Create a handle of the root inode.
    pub fn root_node(&self) -> Node {
        Node {
            nodeid: 1,
            inner: self.root.clone(),
        }
    }

    /// Find an inode by the number and create its handle.
    pub async fn get_node(&self, ino: u64) -> Option<Node> {
        let nodes = self.inner.nodes.lock().await;
        let inner = nodes.get(&ino)?;
        Some(Node {
            nodeid: ino,
            inner: Arc::downgrade(&inner),
        })
    }

    /// Lookup an inode by parent inode number and name.
    ///
    /// This method increments the lookup count for the corresponding node.
    pub async fn lookup(&self, parent: u64, name: &OsStr) -> Option<Node> {
        let nodes = self.inner.nodes.lock().await;
        let parent = nodes.get(&parent)?;
        match parent.kind {
            NodeKind::Dir(ref dir) => {
                let dir = dir.lock().await;
                let nodeid = dir.children.get(name)?.0;
                let inner = nodes.get(&nodeid)?;
                inner.nlookup.fetch_add(1);
                Some(Node {
                    nodeid,
                    inner: Arc::downgrade(&inner),
                })
            }
            _ => None,
        }
    }

    /// Decrease the lookup counts of the specified inodes.
    pub async fn forget(&self, forgets: &[Forget]) {
        let nodes = self.inner.nodes.lock().await;
        for forget in forgets {
            if let Some(node) = nodes.get(&forget.ino()) {
                node.nlookup.fetch_sub(forget.nlookup());
            }
        }
    }

    /// Create a new node onto the specified directory.
    pub async fn new_node(
        &self,
        parent_ino: u64,
        name: OsString,
        attr: FileAttr,
    ) -> Result<Node, i32> {
        let mut nodes = self.inner.nodes.lock().await;
        let parent = nodes
            .get(&parent_ino)
            .cloned()
            .ok_or_else(|| libc::ENOENT)?;
        match parent.kind {
            NodeKind::Dir(ref dir) => {
                let mut dir = dir.lock().await;
                match dir.children.entry(name) {
                    MapEntry::Occupied(..) => Err(libc::EEXIST),
                    MapEntry::Vacant(entry) => {
                        let mut attr = attr;

                        let ino = self.inner.next_ino.fetch_add(1);
                        attr.set_ino(ino);

                        let kind = match attr.mode() & libc::S_IFMT {
                            libc::S_IFDIR => NodeKind::Dir(Mutex::new(DirNode {
                                children: IndexMap::new(),
                                dirents: [
                                    DirEntry::dir(".", ino, 1),
                                    DirEntry::dir("..", parent_ino, 2),
                                ],
                            })),
                            libc::S_IFREG => NodeKind::File,
                            _ => return Err(libc::ENOTSUP),
                        };

                        let inner = Arc::new(NodeInner {
                            attr: AtomicCell::new(attr),
                            kind,
                            nlookup: AtomicCell::new(0),
                        });
                        let inner_ptr = Arc::downgrade(&inner);

                        nodes.insert(ino, inner);

                        let dirent = DirEntry::new(entry.key(), ino, (entry.index() + 3) as u64);
                        entry.insert((ino, dirent));

                        Ok(Node {
                            nodeid: ino,
                            inner: inner_ptr,
                        })
                    }
                }
            }
            _ => Err(libc::ENOTDIR),
        }
    }

    /// Remove an inode from the table.
    pub async fn remove_node(&self, ino: u64) {
        let mut nodes = self.inner.nodes.lock().await;
        nodes.remove(&ino);
    }

    #[doc(hidden)]
    pub async fn reply_readdir<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Readdir<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match self.get_inner(op.ino()).await {
            Some(node) => match node.kind {
                NodeKind::Dir(ref dir) => dir.lock().await.reply_readdir(cx, op).await,
                _ => cx.reply_err(libc::ENOTDIR).await,
            },
            None => cx.reply_err(libc::ENOENT).await,
        }
    }

    async fn get_inner(&self, ino: u64) -> Option<Arc<NodeInner>> {
        let nodes = self.inner.nodes.lock().await;
        nodes.get(&ino).cloned()
    }
}

impl Node {
    /// Return the identifier of the associated inode.
    pub fn nodeid(&self) -> u64 {
        self.nodeid
    }

    /// Return the attribute of the associated inode.
    ///
    /// # Panic
    /// The associated inode has already been dropped.
    pub fn attr(&self) -> FileAttr {
        let inner = self.inner.upgrade().unwrap();
        inner.attr.load()
    }

    pub fn set_attr(&self, attr: FileAttr) {
        let inner = self.inner.upgrade().unwrap();
        inner.attr.store(attr);
    }
}
