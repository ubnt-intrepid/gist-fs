//! In-memory node table.

use crossbeam::atomic::AtomicCell;
use futures::{io::AsyncWrite, lock::Mutex};
use indexmap::map::{Entry as MapEntry, IndexMap};
use polyfuse::{op, Context, DirEntry, FileAttr, Forget};
use std::{
    ffi::{OsStr, OsString},
    io,
    sync::{Arc, Weak},
};

/// In-memory inode table.
///
/// The instance of this type manages the hierarchical structure
/// of inodes in a filesystem.
#[derive(Debug)]
pub struct NodeTable {
    global: Arc<NodeTableInner>,
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
    inner: Weak<NodeInner>,
    global: Weak<NodeTableInner>,
}

#[derive(Debug)]
struct NodeInner {
    nodeid: u64,
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
    children: IndexMap<OsString, (Weak<NodeInner>, DirEntry)>,
    dirents: [DirEntry; 2],
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
            nodeid: 1,
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
            global: Arc::new(NodeTableInner {
                nodes: Mutex::new(nodes),
                next_ino: AtomicCell::new(2), // ino=1 is used by the root inode.
            }),
            root: root_ptr,
        }
    }

    /// Create a handle of the root inode.
    pub fn root(&self) -> Node {
        Node {
            inner: self.root.clone(),
            global: Arc::downgrade(&self.global),
        }
    }

    /// Find an inode by the number and create its handle.
    pub async fn get(&self, ino: u64) -> Option<Node> {
        match ino {
            1 => Some(self.root()),
            ino => Some(Node {
                inner: Arc::downgrade(self.global.nodes.lock().await.get(&ino)?),
                global: Arc::downgrade(&self.global),
            }),
        }
    }

    /// Lookup an inode by parent inode number and name.
    ///
    /// This method increments the lookup count for the corresponding node.
    pub async fn lookup(&self, parent: u64, name: &OsStr) -> Option<Node> {
        let parent = self.global.nodes.lock().await.get(&parent)?.clone();
        match parent.kind {
            NodeKind::Dir(ref dir) => {
                let dir = dir.lock().await;
                let node = &dir.children.get(name)?.0;
                node.upgrade()?.nlookup.fetch_add(1);
                Some(Node {
                    inner: node.clone(),
                    global: Arc::downgrade(&self.global),
                })
            }
            _ => None,
        }
    }

    /// Decrease the lookup counts of the specified inodes.
    pub async fn forget(&self, forgets: &[Forget]) {
        let nodes = self.global.nodes.lock().await;
        for forget in forgets {
            if let Some(node) = nodes.get(&forget.ino()) {
                node.nlookup.fetch_sub(forget.nlookup());
            }
        }
    }
}

impl Node {
    /// Return the identifier of the associated inode.
    ///
    /// This method will cause a panic if the associated inode has already been dropped.
    pub fn nodeid(&self) -> u64 {
        self.inner.upgrade().unwrap().nodeid
    }

    /// Fetch the attribute of the associated inode.
    ///
    /// This method will cause a panic if the associated inode has already been dropped.
    pub fn attr(&self) -> FileAttr {
        let inner = self.inner.upgrade().unwrap();
        inner.attr.load()
    }

    /// Set the attribute of the associated inode.
    ///
    /// This method will cause a panic if the associated inode has already been dropped.
    pub fn set_attr(&self, attr: FileAttr) {
        let inner = self.inner.upgrade().unwrap();
        inner.attr.store(attr);
    }

    /// Create a new node onto the specified directory.
    pub async fn new_child(&self, name: OsString, attr: FileAttr) -> Result<Node, i32> {
        let global = self.global.upgrade().expect("the node table is died");
        let parent = self.inner.upgrade().expect("the node is died");

        let mut attr = attr;
        let ino = global.next_ino.load();
        attr.set_ino(ino);

        let kind = match attr.mode() & libc::S_IFMT {
            libc::S_IFDIR => NodeKind::Dir(Mutex::new(DirNode {
                children: IndexMap::new(),
                dirents: [
                    DirEntry::dir(".", ino, 1),
                    DirEntry::dir("..", parent.nodeid, 2),
                ],
            })),
            libc::S_IFREG => NodeKind::File,
            _ => return Err(libc::ENOTSUP),
        };

        match parent.kind {
            NodeKind::Dir(ref dir) => {
                let mut dir = dir.lock().await;
                match dir.children.entry(name) {
                    MapEntry::Occupied(..) => Err(libc::EEXIST),
                    MapEntry::Vacant(entry) => {
                        global.next_ino.fetch_add(1);
                        let inner = Arc::new(NodeInner {
                            nodeid: ino,
                            attr: AtomicCell::new(attr),
                            kind,
                            nlookup: AtomicCell::new(0),
                        });
                        let inner_ptr = Arc::downgrade(&inner);

                        let mut nodes = global.nodes.lock().await;
                        nodes.insert(ino, inner);

                        let dirent = DirEntry::new(entry.key(), ino, (entry.index() + 3) as u64);
                        entry.insert((inner_ptr.clone(), dirent));

                        Ok(Node {
                            inner: inner_ptr,
                            global: self.global.clone(),
                        })
                    }
                }
            }
            _ => Err(libc::ENOTDIR),
        }
    }

    /// Remove this inode from the table.
    pub async fn remove(&self) {
        let global = self.global.upgrade().unwrap();
        let inner = self.inner.upgrade().unwrap();
        let mut nodes = global.nodes.lock().await;
        nodes.remove(&inner.nodeid);
    }

    pub async fn readdir<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Readdir<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match self.inner.upgrade() {
            Some(node) => match node.kind {
                NodeKind::Dir(ref dir) => dir.lock().await.reply_readdir(cx, op).await,
                _ => cx.reply_err(libc::ENOTDIR).await,
            },
            None => cx.reply_err(libc::ENOENT).await,
        }
    }
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
