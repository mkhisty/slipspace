use clap::Parser;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, Request,
};
use libc::{EIO, ENOENT};
use lru::LruCache;
use sha2::{Digest, Sha256};
use ssh2::{Session, Sftp};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, UNIX_EPOCH};
use crossbeam_channel::{unbounded, Sender, Receiver};

#[derive(Parser, Debug)]
#[command(name = "slipspace", author, version, about = "Slipspace Client FUSE Daemon", long_about = None)]
struct Args {
    /// SSH connection string (e.g., user:password@host:port or user@host)
    ssh_string: String,

    /// Local mount point directory
    local_path: String,

    /// Remote directory path (assumed to be a FUSE mount on the server)
    server_path: String,

    #[arg(short = 'c', long, default_value = "/tmp/sshfs_cache")]
    cache_dir: String,

    #[arg(short = 's', long, default_value = "127.0.0.1:8080")]
    signal_server: String,

    /// Delays background syncing by 15 seconds to test server blocking logic
    #[arg(long)]
    test_delay: bool,
}

fn parse_ssh_string(s: &str) -> (String, Option<String>, String, u16) {
    let parts: Vec<&str> = s.split('@').collect();
    if parts.len() != 2 {
        panic!("Invalid SSH string. Expected user[:password]@host[:port]");
    }

    let up_parts: Vec<&str> = parts[0].splitn(2, ':').collect();
    let user = up_parts[0].to_string();
    let password = if up_parts.len() > 1 { Some(up_parts[1].to_string()) } else { None };

    let hp_parts: Vec<&str> = parts[1].splitn(2, ':').collect();
    let host = hp_parts[0].to_string();
    let port = if hp_parts.len() > 1 { hp_parts[1].parse().unwrap_or(22) } else { 22 };

    (user, password, host, port)
}

struct FileCache {
    cache_dir: PathBuf,
    changed_dir: PathBuf,
    lru: LruCache<PathBuf, u64>,
    remote_root: PathBuf,
    current_size: u64,
    max_size: u64,
    dirty_files: Arc<Mutex<HashSet<PathBuf>>>,
}

impl FileCache {
    fn new(cache_dir: PathBuf, dirty_files: Arc<Mutex<HashSet<PathBuf>>>, remote_root: PathBuf) -> Self {
        let changed_dir = cache_dir.join(".changed");
        fs::create_dir_all(&changed_dir).unwrap();

        Self {
            cache_dir,
            changed_dir,
            lru: LruCache::unbounded(),
            remote_root,
            current_size: 0,
            max_size: 1024 * 1024 * 1024, // 1GB
            dirty_files,
        }
    }

    fn get_local_path(&self, remote_path: &Path) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(remote_path.to_string_lossy().as_bytes());
        let hash = hex::encode(hasher.finalize());
        self.cache_dir.join(hash)
    }

    fn touch(&mut self, remote_path: &Path) {
        self.lru.get(remote_path);
    }

    fn add_file(&mut self, remote_path: PathBuf, size: u64) {
        if let Some(old_size) = self.lru.put(remote_path.clone(), size) {
            self.current_size -= old_size;
        }
        self.current_size += size;
        self.evict_if_needed();
    }

    fn evict_if_needed(&mut self) {
        let mut to_restore = Vec::new();
        while self.current_size > self.max_size {
            if let Some((path, size)) = self.lru.pop_lru() {
                let is_dirty = {
                    let set = self.dirty_files.lock().unwrap();
                    set.contains(&path)
                };
                
                if is_dirty {
                    to_restore.push((path, size));
                    continue;
                }

                let local_path = self.get_local_path(&path);
                let _ = fs::remove_file(local_path);
                self.current_size -= size;
            } else {
                break;
            }
        }
        for (path, size) in to_restore {
            self.lru.put(path, size);
        }
    }

    fn mark_dirty(&mut self, remote_path: &Path, signal_stream: &mut Option<TcpStream>, signal_server: &str) {
        let is_newly_dirty = {
            let mut set = self.dirty_files.lock().unwrap();
            set.insert(remote_path.to_path_buf())
        };

        if is_newly_dirty {
            let rel_path = remote_path.strip_prefix(&self.remote_root).unwrap_or(remote_path);
            let send_path = PathBuf::from("/").join(rel_path);
            let msg = format!("DIRTY {}\n", send_path.display());
            let mut retry = false;
            
            if let Some(stream) = signal_stream {
                if stream.write_all(msg.as_bytes()).is_err() {
                    retry = true;
                }
            } else {
                retry = true;
            }
            
            if retry {
                if let Ok(mut stream) = TcpStream::connect(signal_server) {
                    // Re-send all dirty locks on reconnect!
                    let dirty = {
                        let set = self.dirty_files.lock().unwrap();
                        set.clone()
                    };
                    for d in dirty {
                        let rel_d = d.strip_prefix(&self.remote_root).unwrap_or(&d);
                        let send_d = PathBuf::from("/").join(rel_d);
                        let _ = stream.write_all(format!("DIRTY {}\n", send_d.display()).as_bytes());
                    }
                    *signal_stream = Some(stream);
                }
            }
        }
        let mut hasher = Sha256::new();
        hasher.update(remote_path.to_string_lossy().as_bytes());
        let hash = hex::encode(hasher.finalize());
        let changed_file = self.changed_dir.join(hash);
        let _ = fs::File::create(changed_file);
    }
}

struct SshFs {
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
}

impl SshFs {
    fn new(
        sftp: Sftp,
        remote_path: String,
        cache_dir: PathBuf,
        signal_server: String,
        tx: Sender<(PathBuf, PathBuf)>,
        dirty_files: Arc<Mutex<HashSet<PathBuf>>>,
        versions: Arc<Mutex<HashMap<PathBuf, u64>>>,
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

const TTL: Duration = Duration::from_secs(0);

fn sftp_stat_to_attr(ino: u64, stat: &ssh2::FileStat) -> FileAttr {
    let kind = if stat.is_dir() {
        FileType::Directory
    } else {
        FileType::RegularFile
    };

    let mtime = stat.mtime.unwrap_or(0);
    let atime = stat.atime.unwrap_or(0);

    FileAttr {
        ino,
        size: stat.size.unwrap_or(0),
        blocks: (stat.size.unwrap_or(0) + 511) / 512,
        atime: UNIX_EPOCH + Duration::from_secs(atime),
        mtime: UNIX_EPOCH + Duration::from_secs(mtime),
        ctime: UNIX_EPOCH + Duration::from_secs(mtime),
        crtime: UNIX_EPOCH + Duration::from_secs(mtime),
        kind,
        perm: stat.perm.unwrap_or(0o755) as u16,
        nlink: 1,
        uid: stat.uid.unwrap_or(0),
        gid: stat.gid.unwrap_or(0),
        rdev: 0,
        blksize: 4096,
        flags: 0,
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
                        stat.size = Some(meta.len());
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
                        stat.size = Some(meta.len());
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
                        if r_size != local_stat.len() {
                            download = true;
                        }
                    }
                }
            }
        }

        if download {
            // Fetch file
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
                    }
                }
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
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
                        self.cache.touch(&path);

                        // Increment version tracker on write
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
                // Queue for background asynchronous sync
                let _ = self.tx.send((path, local_path));
            }
        }
        reply.ok();
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let path = parent_path.join(name);

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

        match self.sftp.unlink(&path) {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(EIO),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = self.inodes.get(&parent).cloned().unwrap_or_default();
        let path = parent_path.join(name);

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
        
        match self.sftp.create(&path) {
            Ok(mut remote_file) => {
                let ino = self.get_inode(&path);
                let fh = self.next_fh;
                self.next_fh += 1;
                self.open_files.insert(fh, path.clone());
                
                // Initialize local cache with empty file
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
            // Queue for upload
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

fn main() {
    env_logger::init();
    let args = Args::parse();

    let (user, password, host, port) = parse_ssh_string(&args.ssh_string);
    let remote_path = args.server_path.clone();
    let mount_point = args.local_path.clone();

    println!("Connecting to {}:{} as {}...", host, port, user);

    let tcp = TcpStream::connect(format!("{}:{}", host, port)).expect("Failed to connect to TCP socket");
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();

    if let Some(ref pw) = password {
        sess.userauth_password(&user, pw).unwrap();
    } else {
        sess.userauth_agent(&user).unwrap();
    }

    assert!(sess.authenticated(), "Authentication failed");

    println!("Opening SFTP session...");
    let sftp = sess.sftp().unwrap();

    let (tx, rx) = unbounded::<(PathBuf, PathBuf)>();
    let dirty_files = Arc::new(Mutex::new(HashSet::new()));
    let versions = Arc::new(Mutex::new(HashMap::new()));

    let listen_signal_server = args.signal_server.clone();
    let listen_cache_dir = PathBuf::from(&args.cache_dir);
    let listen_remote_root = PathBuf::from(&remote_path);
    thread::spawn(move || {
        loop {
            if let Ok(mut stream) = TcpStream::connect(&listen_signal_server) {
                if stream.write_all(b"SUBSCRIBE\n").is_ok() {
                    let reader = std::io::BufReader::new(stream);
                    for line in std::io::BufRead::lines(reader) {
                        if let Ok(line) = line {
                            let parts: Vec<&str> = line.splitn(2, ' ').collect();
                            if parts.len() == 2 && parts[0] == "INVALIDATE" {
                                let rel = parts[1].strip_prefix("/").unwrap_or(parts[1]);
                                let remote = listen_remote_root.join(rel);
                                
                                let mut hasher = Sha256::new();
                                hasher.update(remote.to_string_lossy().as_bytes());
                                let hash = hex::encode(hasher.finalize());
                                let local_path = listen_cache_dir.join(&hash);
                                let base_path = local_path.with_extension(format!("{}.base", local_path.extension().unwrap_or_default().to_string_lossy()));
                                
                                println!("[INVALIDATE] Server mutated file {:?}. Purging cache...", remote);
                                let _ = fs::remove_file(&local_path);
                                let _ = fs::remove_file(&base_path);
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
            thread::sleep(Duration::from_secs(1));
        }
    });

    // Spawn Background Uploader Thread
    let bg_host = host.clone();
    let bg_port = port;
    let bg_user = user.clone();
    let bg_password = password.clone();
    let signal_server = args.signal_server.clone();
    let bg_versions = Arc::clone(&versions);
    let bg_dirty = Arc::clone(&dirty_files);
    let test_delay = args.test_delay;
    let bg_remote_root = PathBuf::from(remote_path.clone());

    thread::spawn(move || {
        // Create an independent SFTP connection for the background thread
        let bg_tcp = TcpStream::connect(format!("{}:{}", bg_host, bg_port)).expect("Failed to connect BG sync thread to server");
        let mut bg_sess = Session::new().unwrap();
        bg_sess.set_tcp_stream(bg_tcp);
        bg_sess.handshake().unwrap();
        if let Some(pw) = bg_password {
            bg_sess.userauth_password(&bg_user, &pw).unwrap();
        } else {
            bg_sess.userauth_agent(&bg_user).unwrap();
        }
        let bg_sftp = bg_sess.sftp().unwrap();
        
        let mut bg_signal_stream = TcpStream::connect(&signal_server).ok();

        for (remote_path, local_path) in rx {
            if test_delay {
                println!("TEST MODE: Delaying sync of {:?} by 15 seconds...", remote_path);
                thread::sleep(Duration::from_secs(15));
            }
            
            let start_version = {
                let map = bg_versions.lock().unwrap();
                map.get(&remote_path).cloned().unwrap_or(0)
            };

            println!("Background SFTP sync starting for {:?}", remote_path);

            let base_path = local_path.with_extension(format!("{}.base", local_path.extension().unwrap_or_default().to_string_lossy()));
            let old_data = fs::read(&base_path).unwrap_or_default();
            let new_data = fs::read(&local_path).unwrap_or_default();
            
            let sig_options = fast_rsync::SignatureOptions {
                block_size: 2048,
                crypto_hash_size: 8,
            };
            let sig = fast_rsync::Signature::calculate(&old_data, sig_options);
            let mut delta = Vec::new();
            fast_rsync::diff(&sig.index(), &new_data, &mut delta).unwrap();
            
            let rel_path = remote_path.strip_prefix(&bg_remote_root).unwrap_or(&remote_path);
            let send_path = PathBuf::from("/").join(rel_path);
            let msg = format!("PATCH {} {}\n", send_path.display(), delta.len());
            
            let mut sent_patch = false;
            if let Some(stream) = &mut bg_signal_stream {
                if stream.write_all(msg.as_bytes()).is_ok() && stream.write_all(&delta).is_ok() {
                    sent_patch = true;
                }
            }
            if !sent_patch {
                if let Ok(mut new_stream) = TcpStream::connect(&signal_server) {
                    if new_stream.write_all(msg.as_bytes()).is_ok() && new_stream.write_all(&delta).is_ok() {
                        bg_signal_stream = Some(new_stream);
                        sent_patch = true;
                    } else {
                        bg_signal_stream = None;
                    }
                }
            }
            
            if sent_patch {
                let _ = fs::write(&base_path, &new_data);
            }

            let current_version = {
                let map = bg_versions.lock().unwrap();
                *map.get(&remote_path).unwrap_or(&0)
            };

            if current_version == start_version {
                println!("Sync complete for {:?}. Sending CLEAN signal.", remote_path);
                
                {
                    let mut set = bg_dirty.lock().unwrap();
                    set.remove(&remote_path);
                }

                // We don't bother persisting CLEAN signals as much, since if the server restarts
                // the lock is cleared anyway. We'll just connect and send.
                // But wait! If we connect and drop, the server wipes ALL locks for the BG client!
                let rel_path = remote_path.strip_prefix(&bg_remote_root).unwrap_or(&remote_path);
                let send_path = PathBuf::from("/").join(rel_path);
                let msg = format!("CLEAN {}\n", send_path.display());
                if let Some(stream) = &mut bg_signal_stream {
                    if stream.write_all(msg.as_bytes()).is_err() {
                        if let Ok(mut new_stream) = TcpStream::connect(&signal_server) {
                            let _ = new_stream.write_all(msg.as_bytes());
                            bg_signal_stream = Some(new_stream);
                        } else {
                            bg_signal_stream = None;
                        }
                    }
                } else if let Ok(mut stream) = TcpStream::connect(&signal_server) {
                    let _ = stream.write_all(msg.as_bytes());
                    bg_signal_stream = Some(stream);
                }
            } else {
                println!("File {:?} was modified during upload! Postponing CLEAN signal.", remote_path);
            }
        }
    });

    let fs = SshFs::new(
        sftp, 
        remote_path, 
        PathBuf::from(args.cache_dir), 
        args.signal_server, 
        tx, 
        dirty_files, 
        versions
    );

    let options = vec![
        MountOption::FSName("slipspace".to_string()),
    ];

    let mnt_clone = mount_point.clone();
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl+C! Unmounting FUSE and exiting cleanly...");
        let mnt_path = std::path::Path::new(&mnt_clone);
        let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(mnt_path).output();
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    println!("Mounting slipspace FUSE on {:?}", mount_point);
    
    // Ensure the mount point is completely clear of stale/dead mounts from previous crashes
    let mnt_path = std::path::Path::new(&mount_point);
    let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(mnt_path).output();
    let _ = std::fs::remove_dir_all(mnt_path);
    std::fs::create_dir_all(mnt_path).unwrap();

    fuser::mount2(fs, mount_point, &options).expect("Failed to mount filesystem");
}
