use clap::Parser;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use libc::{EIO, ENOENT};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(name = "server-daemon", author, version, about = "Slipspace Server Daemon", long_about = None)]
struct Args {
    /// The path to the directory that should be intercepted by FUSE
    #[arg(short, long)]
    path: String,
}

struct ServerDaemonFs {
    backing_dir: PathBuf,
    inodes: HashMap<u64, PathBuf>,
    paths: HashMap<PathBuf, u64>,
    next_inode: u64,
    open_files: HashMap<u64, File>,
    next_fh: u64,
    dirty_files: Arc<Mutex<HashSet<PathBuf>>>,
    notify: Arc<Condvar>,
    subscribers: Arc<Mutex<Vec<std::net::TcpStream>>>,
}

impl ServerDaemonFs {
    fn new(backing_dir: PathBuf, dirty_files: Arc<Mutex<HashSet<PathBuf>>>, notify: Arc<Condvar>, subscribers: Arc<Mutex<Vec<std::net::TcpStream>>>) -> Self {
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

    fn is_dirty_any(&self, path1: &PathBuf, path2: &PathBuf) -> bool {
        let set = self.dirty_files.lock().unwrap();
        if set.contains(path1) || set.contains(path2) {
            println!("--> [INTERCEPT] Rejected access on dirty file(s): {:?} / {:?} (EACCES)", path1, path2);
            true
        } else {
            false
        }
    }
}

const TTL: Duration = Duration::from_secs(0);

fn fs_meta_to_attr(ino: u64, meta: &fs::Metadata) -> FileAttr {
    let kind = if meta.is_dir() {
        FileType::Directory
    } else if meta.file_type().is_symlink() {
        FileType::Symlink
    } else {
        FileType::RegularFile
    };

    FileAttr {
        ino,
        size: meta.size(),
        blocks: meta.blocks(),
        atime: UNIX_EPOCH + Duration::from_secs(meta.atime() as u64),
        mtime: UNIX_EPOCH + Duration::from_secs(meta.mtime() as u64),
        ctime: UNIX_EPOCH + Duration::from_secs(meta.ctime() as u64),
        crtime: UNIX_EPOCH + Duration::from_secs(meta.ctime() as u64),
        kind,
        perm: (meta.mode() & 0o777) as u16,
        nlink: meta.nlink() as u32,
        uid: meta.uid(),
        gid: meta.gid(),
        rdev: meta.rdev() as u32,
        blksize: meta.blksize() as u32,
        flags: 0,
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
        let real_path = self.to_real_path(&rel_path);
        
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

        let real_path = self.to_real_path(&rel_path);

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
        
        let real_path = self.to_real_path(&rel_path);

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
        
        let real_path = self.to_real_path(&rel_path);

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
        
        let real_path = self.to_real_path(&rel_path);

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
        
        let real_path = self.to_real_path(&rel_path);

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

fn main() {
    env_logger::init();
    let args = Args::parse();

    let target = PathBuf::from(&args.path);
    let target = target.canonicalize().unwrap_or(target);
    
    let parent = target.parent().unwrap_or(Path::new("/"));
    let name = target.file_name().unwrap_or(OsStr::new("workspace"));
    
    let mut hidden_name = String::from(".slipspace_backing_");
    hidden_name.push_str(name.to_string_lossy().as_ref());
    let backing = parent.join(hidden_name);

    if backing.exists() {
        // Previously initialized. Unmount and recreate in case of stale mount
        let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(&target).output();
        let _ = fs::remove_dir_all(&target);
        fs::create_dir_all(&target).unwrap();
    } else {
        // First time initialization
        if !target.exists() {
            fs::create_dir_all(&backing).unwrap();
            let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(&target).output();
            let _ = fs::remove_dir_all(&target);
            fs::create_dir_all(&target).unwrap();
        } else {
            // Move existing directory to hidden backing store
            fs::rename(&target, &backing).expect("Failed to move target directory to backing store");
            let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(&target).output();
            let _ = fs::remove_dir_all(&target);
            fs::create_dir_all(&target).unwrap();
        }
    }

    let mnt = target;

    let dirty_files = Arc::new(Mutex::new(HashSet::new()));
    let notify = Arc::new(Condvar::new());

    let df_clone = Arc::clone(&dirty_files);
    let notify_clone = Arc::clone(&notify);
    let backing_clone = backing.clone();
    let subscribers: Arc<Mutex<Vec<std::net::TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
    let subscribers_clone = Arc::clone(&subscribers);

    thread::spawn(move || {
        let listener = TcpListener::bind("0.0.0.0:8080").expect("Failed to bind port 8080");
        println!("Server daemon listening for signals on port 8080...");
        
        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let df = Arc::clone(&df_clone);
                let notif = Arc::clone(&notify_clone);
                let bg_backing = backing_clone.clone();
                let subs_clone = Arc::clone(&subscribers_clone);
                thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut client_locks = HashSet::new();
                    let mut line_buf = String::new();
                    
                    loop {
                        line_buf.clear();
                        if std::io::BufRead::read_line(&mut reader, &mut line_buf).unwrap_or(0) == 0 {
                            break;
                        }
                        let line = line_buf.trim_end();
                        let parts: Vec<&str> = line.splitn(3, ' ').collect();
                        if parts.len() >= 1 {
                            let cmd = parts[0];
                            
                            if cmd == "SUBSCRIBE" {
                                subs_clone.lock().unwrap().push(stream.try_clone().unwrap());
                                println!("[SIGNAL] Client subscribed for invalidations.");
                            } else if parts.len() >= 2 {
                                let path = PathBuf::from(parts[1]);
                                
                                if cmd == "DIRTY" {
                                let mut set = df.lock().unwrap();
                                set.insert(path.clone());
                                client_locks.insert(path);
                                println!("[SIGNAL] Marked DIRTY: {:?}", parts[1]);
                            } else if cmd == "CLEAN" {
                                let mut set = df.lock().unwrap();
                                set.remove(&path);
                                client_locks.remove(&path);
                                println!("[SIGNAL] Marked CLEAN: {:?}", parts[1]);
                                notif.notify_all();
                            } else if cmd == "PATCH" && parts.len() == 3 {
                                if let Ok(len) = parts[2].parse::<usize>() {
                                    let mut patch_data = vec![0; len];
                                    if std::io::Read::read_exact(&mut reader, &mut patch_data).is_ok() {
                                        println!("[SIGNAL] Applying patch of size {} to {:?}", len, path);
                                        let rel = path.strip_prefix("/").unwrap_or(&path);
                                        let real_path = bg_backing.join(rel);
                                        let old_data = std::fs::read(&real_path).unwrap_or_default();
                                        let mut new_data = Vec::new();
                                        if fast_rsync::apply(&old_data, &patch_data, &mut new_data).is_ok() {
                                            let _ = std::fs::write(&real_path, &new_data);
                                        }
                                    } else {
                                        break;
                                    }
                                }
                                }
                            }
                        }
                    }
                    
                    // Connection lost! Clean up stale locks for this client
                    if !client_locks.is_empty() {
                        println!("[SIGNAL] Client disconnected! Cleaning up {} stale locks...", client_locks.len());
                        let mut set = df.lock().unwrap();
                        for path in client_locks {
                            set.remove(&path);
                        }
                        notif.notify_all();
                    }
                });
            }
        }
    });

    let fs = ServerDaemonFs::new(backing.clone(), dirty_files, notify, subscribers);

    // By not supplying kernel caching arguments and relying on passthrough defaults,
    // the kernel cache is typically bypassed for data content inside the MVP.
    let mnt_clone = mnt.clone();
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl+C! Unmounting FUSE and exiting cleanly...");
        let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(&mnt_clone).output();
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    let options = vec![
        MountOption::FSName("slipspace".to_string()),
    ];

    println!("Starting Server Daemon...");
    println!("Transparently intercepted: {:?}", mnt);
    println!("Physical backing store: {:?}", backing);
    
    fuser::mount2(fs, mnt, &options).expect("Failed to mount filesystem");
}
