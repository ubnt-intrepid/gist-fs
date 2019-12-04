use indexmap::map::{Entry as MapEntry, IndexMap};
use polyfuse::{DirEntry, FileAttr, Forget};
use std::{
    any::TypeId,
    ffi::{OsStr, OsString},
};

pub const ROOT_INO: u64 = 1;

pub trait Node: Send + Sync + 'static {
    fn attr(&self) -> FileAttr;
    fn store_attr(&self, attr: FileAttr);

    #[doc(hidden)]
    fn __private_type_id__(&self) -> TypeId {
        TypeId::of::<Self>()
    }
}

impl dyn Node {
    fn is<T: Node>(&self) -> bool {
        self.__private_type_id__() == TypeId::of::<T>()
    }

    pub fn downcast_ref<T: Node>(&self) -> Option<&T> {
        if self.is::<T>() {
            Some(unsafe { &*(self as *const dyn Node as *const T) })
        } else {
            None
        }
    }
}

// ==== INode ====

struct INode {
    attr: FileAttr,
    node: Box<dyn Node>,
    kind: INodeKind,
}

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

// ==== Table ====

pub struct INodeTable {
    inodes: IndexMap<u64, INode>,
    next_id: u64,
}

impl INodeTable {
    pub fn new<T: Node>(root: T) -> Self {
        let mut attr = root.attr();
        attr.set_ino(1);
        root.store_attr(attr);

        let mut inodes = IndexMap::new();
        inodes.insert(ROOT_INO, INode::new(attr, root));

        Self {
            inodes,
            next_id: ROOT_INO + 1, // The first inode is root.
        }
    }

    pub fn lookup(&self, parent: u64, name: &OsStr) -> Option<&dyn Node> {
        match self.inodes.get(&parent)?.kind {
            INodeKind::Dir { ref children, .. } => {
                let child = children.get(name)?;
                self.get(*child)
            }
            _ => None,
        }
    }

    pub fn forget(&self, _forgets: &[Forget]) {
        ()
    }

    pub fn get(&self, ino: u64) -> Option<&dyn Node> {
        Some(&*self.inodes.get(&ino)?.node)
    }

    pub fn insert<T: Node>(&mut self, parent: u64, name: OsString, node: T) -> Result<(), i32> {
        match self.inodes.get_mut(&parent).ok_or(libc::ENOENT)? {
            INode {
                kind: INodeKind::Dir { children, .. },
                ..
            } => match children.entry(name) {
                MapEntry::Occupied(..) => return Err(libc::EEXIST),
                MapEntry::Vacant(entry) => {
                    let mut attr = node.attr();
                    attr.set_ino(self.next_id);
                    self.next_id += 1;
                    node.store_attr(attr);

                    entry.insert(attr.ino());
                    self.inodes.insert(attr.ino(), INode::new(attr, node));

                    Ok(())
                }
            },
            _ => return Err(libc::ENOTDIR),
        }
    }

    pub fn read_dir(&self, ino: u64, offset: u64, bufsize: usize) -> Result<Vec<DirEntry>, i32> {
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
