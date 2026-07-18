use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use libc::{EIO, ENOENT};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use crate::metadata::{fs_meta_to_attr, TTL};

pub struct ServerDaemonFs {
    backing_dir: PathBuf,
    inodes: HashMap<u64, PathBuf>,
    paths: HashMap<PathBuf, u64>,
    next_inode: u64,
    open_files: HashMap<u64, File>,
    next_fh: u64,
    dirty_files: Arc<Mutex<HashSet<PathBuf>>>,
    pub notify: Arc<Condvar>,
    subscribers: Arc<Mutex<Vec<std::net::TcpStream>>>,
}

impl ServerDaemonFs {
    pub fn new(backing_dir: PathBuf, dirty_files: Arc<Mutex<HashSet<PathBuf>>>, notify: Arc<Condvar>, subscribers: Arc<Mutex<Vec<std::net::TcpStream>>>) -> Self {
        let mut inodes = HashMap::new();
        let mut paths = HashMap::new();
        let root_path = PathBuf::from("/");

        inodes.insert(1, root_path.clone());
        paths.insert(root_path, 1);

        Self {
            backing_dir,
            inodes,
            paths,
            next_inode: 2,
            open_files: HashMap::new(),
            next_fh: 1,
            dirty_files,
            notify,
            subscribers,
        }
    }

    fn get_inode(&mut self, relative_path: &Path) -> u64 {
        if let Some(&inode) = self.paths.get(relative_path) {
            inode
        } else {
            let inode = self.next_inode;
            self.next_inode += 1;
            self.inodes.insert(inode, relative_path.to_path_buf());
            self.paths.insert(relative_path.to_path_buf(), inode);
            inode
        }
    }

    fn invalidate_client(&self, rel_path: &PathBuf) {
        let is_dirty = {
            let set = self.dirty_files.lock().unwrap();
            set.contains(rel_path)
        };
        // Don't invalidate if the client itself is dirtying the file
        if !is_dirty {
            let mut subs = self.subscribers.lock().unwrap();
            let msg = format!("INVALIDATE {}\n", PathBuf::from("/").join(rel_path).display());
            println!("[SERVER] Broadcasting INVALIDATE for {:?}", rel_path);
            subs.retain_mut(|stream| stream.write_all(msg.as_bytes()).is_ok());
        }
    }

    fn to_real_path(&self, rel_path: &Path) -> PathBuf {
        let mut real = self.backing_dir.clone();
        if !rel_path.as_os_str().is_empty() && rel_path != Path::new("/") {
            real.push(rel_path.strip_prefix("/").unwrap_or(rel_path));
        }
        real
    }

    fn is_dirty(&self, rel_path: &PathBuf) -> bool {
        let set = self.dirty_files.lock().unwrap();
        if set.contains(rel_path) {
            println!("--> [INTERCEPT] Rejected access on dirty file: {:?} (EACCES)", rel_path);
            true
        } else {
            false
        }
    }
}

impl Filesystem for ServerDaemonFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match self.inodes.get(&parent) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let rel_path = parent_path.join(name);
        
        if self.is_dirty(&rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        let real_path = self.to_real_path(&rel_path);

        match fs::metadata(&real_path) {
            Ok(meta) => {
                let ino = self.get_inode(&rel_path);
                let attr = fs_meta_to_attr(ino, &meta);
                reply.entry(&TTL, &attr, 0);
            }
            Err(_) => reply.error(ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let rel_path = match self.inodes.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let real_path = self.to_real_path(&rel_path);

        match fs::metadata(&real_path) {
            Ok(meta) => {
                let attr = fs_meta_to_attr(ino, &meta);
                reply.attr(&TTL, &attr);
            }
            Err(_) => reply.error(ENOENT),
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
        let rel_path = match self.inodes.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let real_path = self.to_real_path(&rel_path);

        if let Ok(entries) = fs::read_dir(&real_path) {
            let mut all_entries = vec![
                (PathBuf::from("."), fs::metadata(&real_path).ok()),
                (PathBuf::from(".."), fs::metadata(real_path.parent().unwrap_or(&real_path)).ok()),
            ];

            for entry in entries.flatten() {
                all_entries.push((PathBuf::from(entry.file_name()), entry.metadata().ok()));
            }

            for (i, (name_path, meta_opt)) in all_entries.iter().enumerate() {
                let entry_idx = (i + 1) as i64;
                if entry_idx <= offset {
                    continue;
                }

                if let Some(meta) = meta_opt {
                    let entry_rel = if i == 0 {
                        rel_path.clone()
                    } else if i == 1 {
                        rel_path.parent().unwrap_or(&rel_path).to_path_buf()
                    } else {
                        rel_path.join(name_path)
                    };

                    let entry_ino = self.get_inode(&entry_rel);
                    let kind = if meta.is_dir() {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };

                    let name = if i < 2 {
                        name_path.as_os_str()
                    } else {
                        name_path.file_name().unwrap_or(OsStr::new(""))
                    };

                    let full = reply.add(entry_ino, entry_idx, kind, name);
                    if full {
                        break;
                    }
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        let rel_path = match self.inodes.get(&ino) {
            Some(p) => p.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let real_path = self.to_real_path(&rel_path);

        let mut opts = OpenOptions::new();
        let write_access = (flags & libc::O_ACCMODE) != libc::O_RDONLY;
        let read_access = (flags & libc::O_ACCMODE) == libc::O_RDONLY || (flags & libc::O_ACCMODE) == libc::O_RDWR;
        
        if read_access && self.is_dirty(&rel_path) {
            println!("--> [INTERCEPT] Rejected access on dirty file: {:?}", rel_path);
            reply.error(libc::EACCES);
            return;
        }
        
        opts.read(true);
        if write_access {
            opts.write(true);
        }

        match opts.open(&real_path) {
            Ok(file) => {
                let fh = self.next_fh;
                self.next_fh += 1;
                self.open_files.insert(fh, file);
                reply.opened(fh, 0);
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let rel_path = self.inodes.get(&ino).cloned().unwrap_or_default();
        
        if self.is_dirty(&rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        if let Some(file) = self.open_files.get_mut(&fh) {
            if file.seek(SeekFrom::Start(offset as u64)).is_ok() {
                let mut buffer = vec![0; size as usize];
                if let Ok(bytes_read) = file.read(&mut buffer) {
                    reply.data(&buffer[..bytes_read]);
                    return;
                }
            }
        }
        reply.error(EIO);
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let rel_path = self.inodes.get(&ino).cloned().unwrap_or_default();
        
        if self.is_dirty(&rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        if let Some(file) = self.open_files.get_mut(&fh) {
            if file.seek(SeekFrom::Start(offset as u64)).is_ok() {
                if let Ok(bytes_written) = file.write(data) {
                    reply.written(bytes_written as u32);
                    self.invalidate_client(&rel_path);
                    return;
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
        self.open_files.remove(&fh);
        reply.ok();
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, _umask: u32, reply: ReplyEntry) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let rel_path = parent_path.join(name);
        
        if self.is_dirty(&rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        let real_path = self.to_real_path(&rel_path);
        match fs::create_dir(&real_path) {
            Ok(_) => {
                let ino = self.get_inode(&rel_path);
                if let Ok(meta) = fs::metadata(&real_path) {
                    reply.entry(&TTL, &fs_meta_to_attr(ino, &meta), 0);
                    self.invalidate_client(&rel_path);
                } else {
                    reply.error(ENOENT);
                }
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let rel_path = parent_path.join(name);
        
        if self.is_dirty(&rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        let real_path = self.to_real_path(&rel_path);
        match fs::remove_file(&real_path) {
            Ok(_) => {
                reply.ok();
                self.invalidate_client(&rel_path);
            },
            Err(_) => reply.error(EIO),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let rel_path = parent_path.join(name);
        
        if self.is_dirty(&rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        let real_path = self.to_real_path(&rel_path);
        match fs::remove_dir(&real_path) {
            Ok(_) => {
                reply.ok();
                self.invalidate_client(&rel_path);
            },
            Err(_) => reply.error(EIO),
        }
    }

    fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let rel_path = parent_path.join(name);
        
        let newparent_path = self.inodes.get(&newparent).cloned().unwrap_or_default();
        let new_rel_path = newparent_path.join(newname);
        
        if self.is_dirty(&rel_path) || self.is_dirty(&new_rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        let real_path = self.to_real_path(&rel_path);
        let new_real_path = self.to_real_path(&new_rel_path);
        match fs::rename(&real_path, &new_real_path) {
            Ok(_) => {
                reply.ok();
                self.invalidate_client(&rel_path);
                self.invalidate_client(&new_rel_path);
            },
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
        let rel_path = parent_path.join(name);
        
        if self.is_dirty(&rel_path) {
            reply.error(libc::EACCES);
            return;
        }

        let real_path = self.to_real_path(&rel_path);
        match fs::File::create(&real_path) {
            Ok(file) => {
                let ino = self.get_inode(&rel_path);
                let fh = self.next_fh;
                self.next_fh += 1;
                self.open_files.insert(fh, file);
                
                if let Ok(meta) = fs::metadata(&real_path) {
                    reply.created(&TTL, &fs_meta_to_attr(ino, &meta), 0, fh, 0);
                    self.invalidate_client(&rel_path);
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
        let rel_path = self.inodes.get(&ino).cloned().unwrap_or_default();
        let real_path = self.to_real_path(&rel_path);

        if let Some(s) = size {
            if let Ok(file) = OpenOptions::new().write(true).open(&real_path) {
                let _ = file.set_len(s);
            }
        }
        
        if let Ok(meta) = fs::metadata(&real_path) {
            reply.attr(&TTL, &fs_meta_to_attr(ino, &meta));
        } else {
            reply.error(ENOENT);
        }
    }
}
