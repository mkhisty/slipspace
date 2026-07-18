use lru::LruCache;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct FileCache {
    cache_dir: PathBuf,
    changed_dir: PathBuf,
    lru: LruCache<PathBuf, u64>,
    remote_root: PathBuf,
    current_size: u64,
    max_size: u64,
    pub dirty_files: Arc<Mutex<HashSet<PathBuf>>>,
}

impl FileCache {
    pub fn new(cache_dir: PathBuf, dirty_files: Arc<Mutex<HashSet<PathBuf>>>, remote_root: PathBuf) -> Self {
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

    pub fn get_local_path(&self, remote_path: &Path) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(remote_path.to_string_lossy().as_bytes());
        let hash = hex::encode(hasher.finalize());
        self.cache_dir.join(hash)
    }

    pub fn touch(&mut self, remote_path: &Path) {
        self.lru.get(remote_path);
    }

    pub fn add_file(&mut self, remote_path: PathBuf, size: u64) {
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
                let _ = fs::remove_file(&local_path);
                
                let base_path = local_path.with_extension(format!("{}.base", local_path.extension().unwrap_or_default().to_string_lossy()));
                let _ = fs::remove_file(&base_path);
                
                self.current_size -= size;
            } else {
                break;
            }
        }
        for (path, size) in to_restore {
            self.lru.put(path, size);
        }
    }

    pub fn update_size(&mut self, remote_path: &Path, new_size: u64) {
        let os = self.lru.get(remote_path).copied().unwrap_or(0);
        if self.current_size >= os {
            self.current_size -= os;
        }
        self.current_size += new_size;
        self.lru.put(remote_path.to_path_buf(), new_size);
        self.evict_if_needed();
    }

    pub fn mark_dirty(&mut self, remote_path: &Path, signal_stream: &mut Option<TcpStream>, signal_server: &str) {
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
