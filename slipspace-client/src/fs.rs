use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, Request,
};
use libc::{EIO, ENOENT};
use ssh2::Sftp;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use crossbeam_channel::Sender;

use crate::cache::FileCache;
use crate::metadata::{sftp_stat_to_attr, TTL};

pub struct SshFs {
    sftp: Sftp,
    inodes: HashMap<u64, PathBuf>,
    paths: HashMap<PathBuf, u64>,
    next_inode: u64,
    cache: FileCache,
    open_files: HashMap<u64, PathBuf>,
    next_fh: u64,
    signal_server: String,
    signal_stream: Option<TcpStream>,
    tx: Sender<(PathBuf, PathBuf)>,
    versions: Arc<Mutex<HashMap<PathBuf, u64>>>,
    pending_ignores: Arc<Mutex<HashSet<PathBuf>>>,
}

impl SshFs {
    pub fn new(
        sftp: Sftp,
        remote_path: String,
        cache_dir: PathBuf,
        signal_server: String,
        tx: Sender<(PathBuf, PathBuf)>,
        dirty_files: Arc<Mutex<std::collections::HashSet<PathBuf>>>,
        versions: Arc<Mutex<HashMap<PathBuf, u64>>>,
        pending_ignores: Arc<Mutex<HashSet<PathBuf>>>,
    ) -> Self {
        let mut inodes = HashMap::new();
        let mut paths = HashMap::new();
        let root_path = PathBuf::from(remote_path);

        inodes.insert(1, root_path.clone());
        paths.insert(root_path.clone(), 1);

        Self {
            sftp,
            inodes,
            paths,
            next_inode: 2,
            cache: FileCache::new(cache_dir, dirty_files, root_path),
            open_files: HashMap::new(),
            next_fh: 1,
            signal_server,
            signal_stream: None,
            tx,
            versions,
            pending_ignores,
        }
    }

    fn get_inode(&mut self, path: &Path) -> u64 {
        if let Some(&inode) = self.paths.get(path) {
            inode
        } else {
            let inode = self.next_inode;
            self.next_inode += 1;
            self.inodes.insert(inode, path.to_path_buf());
            self.paths.insert(path.to_path_buf(), inode);
            inode
        }
    }
}

impl Filesystem for SshFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match self.inodes.get(&parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let path = parent_path.join(name);
        match self.sftp.stat(&path) {
            Ok(mut stat) => {
                let is_dirty = {
                    let set = self.cache.dirty_files.lock().unwrap();
                    set.contains(&path)
                };
                if is_dirty {
                    let local_path = self.cache.get_local_path(&path);
                    if let Ok(meta) = fs::metadata(&local_path) {
                        stat.size = Some(meta.len() as u64);
                    }
                }
                let ino = self.get_inode(&path);
                let attr = sftp_stat_to_attr(ino, &stat);
                reply.entry(&TTL, &attr, 0);
            }
            Err(_) => {
                reply.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let path = match self.inodes.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.sftp.stat(&path) {
            Ok(mut stat) => {
                let is_dirty = {
                    let set = self.cache.dirty_files.lock().unwrap();
                    set.contains(&path)
                };
                if is_dirty {
                    let local_path = self.cache.get_local_path(&path);
                    if let Ok(meta) = fs::metadata(&local_path) {
                        stat.size = Some(meta.len() as u64);
                    }
                }
                let attr = sftp_stat_to_attr(ino, &stat);
                reply.attr(&TTL, &attr);
            }
            Err(_) => {
                reply.error(ENOENT);
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.inodes.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.sftp.readdir(&path) {
            Ok(entries) => {
                let mut all_entries = vec![
                    (
                        PathBuf::from("."),
                        ssh2::FileStat {
                            size: None,
                            uid: None,
                            gid: None,
                            perm: Some(0o755),
                            mtime: None,
                            atime: None,
                        },
                    ),
                    (
                        PathBuf::from(".."),
                        ssh2::FileStat {
                            size: None,
                            uid: None,
                            gid: None,
                            perm: Some(0o755),
                            mtime: None,
                            atime: None,
                        },
                    ),
                ];

                all_entries.extend(entries);

                for (i, (entry_path, stat)) in all_entries.iter().enumerate() {
                    let entry_idx = (i + 1) as i64;
                    if entry_idx <= offset {
                        continue;
                    }

                    let name = if i < 2 {
                        entry_path.as_os_str()
                    } else {
                        entry_path.file_name().unwrap_or(OsStr::new(""))
                    };

                    let entry_ino = if i == 0 {
                        ino
                    } else if i == 1 {
                        1
                    } else {
                        self.get_inode(entry_path)
                    };

                    let kind = if stat.is_dir() {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };

                    let full = reply.add(entry_ino, entry_idx, kind, name);
                    if full {
                        break;
                    }
                }
                reply.ok();
            }
            Err(_) => {
                reply.error(ENOENT);
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        let path = match self.inodes.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let local_path = self.cache.get_local_path(&path);
        
        let is_dirty = {
            let set = self.cache.dirty_files.lock().unwrap();
            set.contains(&path)
        };

        let mut download = false;
        if !local_path.exists() {
            download = true;
        } else if !is_dirty {
            if let Ok(remote_stat) = self.sftp.stat(&path) {
                if let Ok(local_stat) = fs::metadata(&local_path) {
                    if let Some(r_size) = remote_stat.size {
                        if r_size != local_stat.len() as u64 {
                            download = true;
                        }
                    }
                }
            }
        }

        if download {
            match self.sftp.open(&path) {
                Ok(mut remote_file) => {
                    if let Some(parent) = local_path.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let mut local_file = fs::File::create(&local_path).unwrap();
                    let mut buffer = Vec::new();
                    if remote_file.read_to_end(&mut buffer).is_ok() {
                        local_file.write_all(&buffer).unwrap();
                        let base_path = local_path.with_extension(format!("{}.base", local_path.extension().unwrap_or_default().to_string_lossy()));
                        let _ = fs::copy(&local_path, &base_path);
                        self.cache.add_file(path.clone(), buffer.len() as u64);
                    }
                }
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        } else {
            self.cache.touch(&path);
        }

        let fh = self.next_fh;
        self.next_fh += 1;
        self.open_files.insert(fh, path);
        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if let Some(path) = self.open_files.get(&fh).cloned() {
            let local_path = self.cache.get_local_path(&path);
            if let Ok(mut file) = fs::File::open(&local_path) {
                if file.seek(SeekFrom::Start(offset as u64)).is_ok() {
                    let mut buffer = vec![0; size as usize];
                    if let Ok(bytes_read) = file.read(&mut buffer) {
                        self.cache.touch(&path);
                        reply.data(&buffer[..bytes_read]);
                        return;
                    }
                }
            }
        }
        reply.error(EIO);
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        if let Some(path) = self.open_files.get(&fh).cloned() {
            let local_path = self.cache.get_local_path(&path);
            if let Ok(mut file) = OpenOptions::new().write(true).open(&local_path) {
                if file.seek(SeekFrom::Start(offset as u64)).is_ok() {
                    if let Ok(bytes_written) = file.write(data) {
                        self.cache.mark_dirty(&path, &mut self.signal_stream, &self.signal_server);
                        if let Ok(meta) = file.metadata() {
                            self.cache.update_size(&path, meta.len());
                        } else {
                            self.cache.touch(&path);
                        }

                        {
                            let mut map = self.versions.lock().unwrap();
                            let v = map.entry(path.clone()).or_insert(0);
                            *v += 1;
                        }

                        reply.written(bytes_written as u32);
                        return;
                    }
                }
            }
        }
        reply.error(EIO);
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if let Some(path) = self.open_files.remove(&fh) {
            let is_dirty = {
                let set = self.cache.dirty_files.lock().unwrap();
                set.contains(&path)
            };
            if is_dirty {
                let local_path = self.cache.get_local_path(&path);
                let _ = self.tx.send((path, local_path));
            }
        }
        reply.ok();
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let path = parent_path.join(name);

        self.pending_ignores.lock().unwrap().insert(path.clone());
        match self.sftp.mkdir(&path, mode as i32) {
            Ok(_) => {
                let ino = self.get_inode(&path);
                if let Ok(stat) = self.sftp.stat(&path) {
                    reply.entry(&TTL, &sftp_stat_to_attr(ino, &stat), 0);
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let path = parent_path.join(name);

        let local_path = self.cache.get_local_path(&path);
        let _ = fs::remove_file(&local_path);

        self.pending_ignores.lock().unwrap().insert(path.clone());
        match self.sftp.unlink(&path) {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(EIO),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let path = parent_path.join(name);

        self.pending_ignores.lock().unwrap().insert(path.clone());
        match self.sftp.rmdir(&path) {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(EIO),
        }
    }

    fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let path = parent_path.join(name);
        
        let newparent_path = self.inodes.get(&newparent).cloned().unwrap_or_default();
        let newpath = newparent_path.join(newname);

        let local_old = self.cache.get_local_path(&path);
        let local_new = self.cache.get_local_path(&newpath);
        let _ = fs::rename(&local_old, &local_new);

        self.pending_ignores.lock().unwrap().insert(path.clone());
        self.pending_ignores.lock().unwrap().insert(newpath.clone());
        match self.sftp.rename(&path, &newpath, None) {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(EIO),
        }
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let path = parent_path.join(name);
        
        self.pending_ignores.lock().unwrap().insert(path.clone());
        match self.sftp.create(&path) {
            Ok(mut remote_file) => {
                let ino = self.get_inode(&path);
                let fh = self.next_fh;
                self.next_fh += 1;
                self.open_files.insert(fh, path.clone());
                
                let local_path = self.cache.get_local_path(&path);
                if let Some(parent) = local_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                let _ = fs::File::create(&local_path);
                let base_path = local_path.with_extension(format!("{}.base", local_path.extension().unwrap_or_default().to_string_lossy()));
                let _ = fs::File::create(&base_path);
                
                if let Ok(stat) = remote_file.stat() {
                    reply.created(&TTL, &sftp_stat_to_attr(ino, &stat), 0, fh, 0);
                } else {
                    reply.error(ENOENT);
                }
            },
            Err(_) => reply.error(EIO),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let path = self.inodes.get(&ino).cloned().unwrap_or_default();
        
        if let Some(s) = size {
            let local_path = self.cache.get_local_path(&path);
            if let Ok(file) = OpenOptions::new().write(true).open(&local_path) {
                let _ = file.set_len(s);
            }
            let _ = self.tx.send((path.clone(), local_path));
            self.cache.mark_dirty(&path, &mut self.signal_stream, &self.signal_server);
        }

        if let Ok(stat) = self.sftp.stat(&path) {
            reply.attr(&TTL, &sftp_stat_to_attr(ino, &stat));
        } else {
            reply.error(ENOENT);
        }
    }

}
