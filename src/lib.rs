//! Gist as a filesystem.

#![allow(dead_code)]

use chrono::{DateTime, Duration, Utc};
use futures::{io::AsyncWrite, lock::Mutex};
use gist_client::{Client, ETag, Gist};
use polyfuse::{op, reply, Context, DirEntry, FileAttr, Filesystem, Operation};
use std::{collections::HashMap, io, os::unix::ffi::OsStrExt};

const ROOT_INO: u64 = 1;

#[derive(Debug)]
pub struct GistFs {
    client: Client,
    gist_id: String,
    cache_period: Duration,
    root_attr: FileAttr,
    state: Mutex<GistFsState>,
}

#[derive(Debug)]
struct GistFsState {
    files: HashMap<u64, GistFile>,
    entries: Vec<DirEntry>,

    gist: Gist,
    etag: Option<ETag>,
    expired: DateTime<Utc>,
}

#[derive(Debug)]
struct GistFile {
    attr: FileAttr,
    filename: String,
    raw_url: String,
    content: Option<Vec<u8>>,
}

impl GistFs {
    pub async fn new(client: Client, gist_id: String) -> anyhow::Result<Self> {
        let (gist, etag) = client
            .fetch_gist(&gist_id, None)
            .await?
            .expect("ETag is set");

        let ctime = gist.created_at;
        let mtime = gist.updated_at;
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        let cache_period = Duration::minutes(5);

        let mut root_attr = FileAttr::default();
        root_attr.set_ino(ROOT_INO);
        root_attr.set_nlink(2);
        root_attr.set_mode(libc::S_IFDIR | 0o755);
        root_attr.set_uid(uid);
        root_attr.set_gid(gid);
        root_attr.set_ctime(ctime.timestamp() as u64, ctime.timestamp_subsec_nanos());
        root_attr.set_mtime(mtime.timestamp() as u64, mtime.timestamp_subsec_nanos());

        let mut files = HashMap::new();
        let mut next_ino = 2;

        for file in gist.files.values() {
            let mut attr = FileAttr::default();
            attr.set_ino(next_ino);
            attr.set_mode(libc::S_IFREG | 0o644);
            attr.set_nlink(1);
            attr.set_size(file.size);
            attr.set_uid(uid);
            attr.set_gid(gid);
            attr.set_ctime(ctime.timestamp() as u64, ctime.timestamp_subsec_nanos());
            attr.set_mtime(mtime.timestamp() as u64, mtime.timestamp_subsec_nanos());

            files.insert(
                next_ino,
                GistFile {
                    attr,
                    filename: file.filename.clone(),
                    raw_url: file.raw_url.clone(),
                    content: if !file.truncated {
                        Some(file.content.clone().into())
                    } else {
                        None
                    },
                },
            );
            next_ino += 1;
        }

        let mut entries = Vec::with_capacity(2 + files.len());
        entries.push(DirEntry::dir(".", ROOT_INO, 1));
        entries.push(DirEntry::dir("..", ROOT_INO, 2));
        for (i, file) in files.values().enumerate() {
            let offset = (i + 3) as u64;
            entries.push(DirEntry::file(&file.filename, file.attr.ino(), offset));
        }

        let expired = Utc::now() + cache_period;

        Ok(Self {
            client,
            gist_id,
            cache_period,
            root_attr,
            state: Mutex::new(GistFsState {
                files,
                entries,
                gist,
                etag,
                expired,
            }),
        })
    }

    pub fn set_cache_period(&mut self, period: impl Into<Duration>) {
        self.cache_period = period.into();
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn fetch(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;

        let now = Utc::now();
        if now <= state.expired {
            tracing::trace!("the cache has not expired yet. do nothing");
            return Ok(());
        }

        let expired = now + self.cache_period;
        tracing::trace!("update the expired time: {}", expired);
        state.expired = expired;

        match self
            .client
            .fetch_gist(&self.gist_id, state.etag.as_ref())
            .await?
        {
            Some((gist, etag)) => {
                tracing::trace!("receive the Gist content: {:?}", gist);

                let ctime = gist.created_at;
                let mtime = gist.updated_at;

                for (filename, file) in &gist.files {
                    match state
                        .files
                        .iter_mut()
                        .find(|(_ino, f)| f.filename == *filename)
                    {
                        Some((_ino, f)) => {
                            f.attr.set_ctime(
                                ctime.timestamp() as u64,
                                ctime.timestamp_subsec_nanos(),
                            );
                            f.attr.set_mtime(
                                mtime.timestamp() as u64,
                                mtime.timestamp_subsec_nanos(),
                            );
                            f.attr.set_size(file.size);
                            if !file.truncated {
                                f.content.replace(file.content.clone().into());
                            }
                        }
                        None => {
                            tracing::warn!("ignore a newly added file: {}", filename);
                            continue;
                        }
                    }
                }

                state.gist = gist;
                state.etag = etag;
            }
            None => {
                tracing::trace!("the content is not modified. do nothing");
                return Ok(());
            }
        }

        Ok(())
    }

    async fn do_lookup<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Lookup<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let state = self.state.lock().await;

        let attr = match op.parent() {
            ROOT_INO => match state
                .files
                .iter()
                .find(|(_ino, f)| f.filename.as_bytes() == op.name().as_bytes())
            {
                Some((_ino, file)) => file.attr,
                None => return cx.reply_err(libc::ENOENT).await,
            },
            _ => return cx.reply_err(libc::ENOENT).await,
        };

        op.reply(cx, reply::ReplyEntry::new(attr)).await
    }

    async fn do_getattr<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Getattr<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let state = self.state.lock().await;

        let attr = match op.ino() {
            ROOT_INO => self.root_attr,
            ino => match state.files.get(&ino) {
                Some(file) => file.attr,
                None => return cx.reply_err(libc::ENOENT).await,
            },
        };

        op.reply(cx, reply::ReplyAttr::new(attr)).await
    }

    async fn do_setattr<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Setattr<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let attr = match op.ino() {
            ROOT_INO => return cx.reply_err(libc::EPERM).await,
            ino => {
                let mut state = self.state.lock().await;
                match state.files.get_mut(&ino) {
                    Some(file) => {
                        if let Some((sec, nsec, is_now)) = op.mtime() {
                            // utimens
                            if is_now {
                                let now = Utc::now();
                                file.attr.set_mtime(
                                    now.timestamp() as u64,
                                    now.timestamp_subsec_nanos(),
                                );
                            } else {
                                file.attr.set_mtime(sec, nsec);
                            };
                        }

                        if let Some(size) = op.size() {
                            // truncate
                            file.attr.set_size(size);
                            if let Some(ref mut content) = file.content {
                                content.resize(size as usize, 0);
                            }
                        }
                        file.attr
                    }
                    None => return cx.reply_err(libc::ENOENT).await,
                }
            }
        };

        op.reply(cx, reply::ReplyAttr::new(attr)).await
    }

    async fn do_opendir<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Opendir<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        if let Err(err) = self.fetch().await {
            tracing::error!("failed to fetch Gist: {}", err);
            return cx.reply_err(libc::EIO).await;
        }
        op.reply(cx, reply::ReplyOpendir::new(0)).await
    }

    async fn do_readdir<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Readdir<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let state = self.state.lock().await;

        let mut total_len = 0;
        let entries: Vec<&[u8]> = state
            .entries
            .iter()
            .skip(op.offset() as usize)
            .map(|entry| entry.as_ref())
            .take_while(|entry| {
                total_len += entry.len() as u32;
                total_len <= op.size()
            })
            .collect();

        op.reply_vectored(cx, &entries[..]).await
    }

    async fn do_open<W: ?Sized>(&self, cx: &mut Context<'_, W>, op: op::Open<'_>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match op.ino() {
            ROOT_INO => return cx.reply_err(libc::EISDIR).await,
            ino => {
                let mut state = self.state.lock().await;
                match state.files.get_mut(&ino) {
                    Some(file) => {
                        // TODO: fetch content.
                        file.content.get_or_insert_with(|| {
                            tracing::warn!("the content of Gist file is truncated.");
                            Vec::new()
                        });
                    }
                    None => return cx.reply_err(libc::ENOENT).await,
                }
            }
        };

        op.reply(cx, reply::ReplyOpen::new(0)).await
    }

    async fn do_read<W: ?Sized>(&self, cx: &mut Context<'_, W>, op: op::Read<'_>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match op.ino() {
            ROOT_INO => cx.reply_err(libc::EISDIR).await,
            ino => {
                let state = self.state.lock().await;
                match state.files.get(&ino) {
                    Some(file) => {
                        let content = file
                            .content
                            .as_ref()
                            .expect("the Gist content is not prepared");
                        let offset = op.offset() as usize;
                        if offset > content.len() {
                            return op.reply(cx, &[]).await;
                        }

                        let content = &content[offset..];
                        let content = &content[..std::cmp::min(content.len(), op.size() as usize)];
                        op.reply(cx, content).await
                    }
                    None => cx.reply_err(libc::ENOENT).await,
                }
            }
        }
    }

    async fn do_write<W: ?Sized, T>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Write<'_>,
        data: T,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        T: AsRef<[u8]>,
    {
        match op.ino() {
            ROOT_INO => cx.reply_err(libc::EISDIR).await,
            ino => {
                let mut state = self.state.lock().await;
                match state.files.get_mut(&ino) {
                    Some(file) => {
                        let content = file
                            .content
                            .as_mut()
                            .expect("The gist content is not prepared");
                        let offset = op.offset() as usize;
                        let size = op.size() as usize;
                        content.resize(offset + size, 0);

                        content[offset..size].copy_from_slice(data.as_ref());

                        file.attr.set_size(content.len() as u64);

                        let size = op.size();
                        op.reply(cx, reply::ReplyWrite::new(size)).await
                    }
                    None => cx.reply_err(libc::ENOENT).await,
                }
            }
        }
    }

    async fn do_flush<W: ?Sized>(
        &self,
        cx: &mut Context<'_, W>,
        op: op::Flush<'_>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match op.ino() {
            ROOT_INO => cx.reply_err(libc::EISDIR).await,
            ino => {
                let mut state = self.state.lock().await;
                match state.files.get_mut(&ino) {
                    Some(_file) => {
                        // TODO
                        tracing::trace!("send modification to GitHub");
                        op.reply(cx).await
                    }
                    None => cx.reply_err(libc::ENOENT).await,
                }
            }
        }
    }
}

#[polyfuse::async_trait]
impl<T> Filesystem<T> for GistFs
where
    T: AsRef<[u8]>,
{
    async fn call<W: ?Sized>(&self, cx: &mut Context<'_, W>, op: Operation<'_, T>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin + Send,
        T: Send + 'async_trait,
    {
        match op {
            Operation::Lookup(op) => self.do_lookup(cx, op).await,
            Operation::Getattr(op) => self.do_getattr(cx, op).await,
            Operation::Setattr(op) => self.do_setattr(cx, op).await,
            Operation::Opendir(op) => self.do_opendir(cx, op).await,
            Operation::Readdir(op) => self.do_readdir(cx, op).await,
            Operation::Open(op) => self.do_open(cx, op).await,
            Operation::Read(op) => self.do_read(cx, op).await,
            Operation::Write(op, data) => self.do_write(cx, op, data).await,
            Operation::Flush(op) => self.do_flush(cx, op).await,
            _ => Ok(()),
        }
    }
}
