use sha2::{Digest, Sha256};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

pub fn run_invalidation_listener(
    listen_signal_server: String,
    listen_cache_dir: PathBuf,
    listen_remote_root: PathBuf,
    listen_dirty: Arc<Mutex<HashSet<PathBuf>>>,
    listen_ignores: Arc<Mutex<HashSet<PathBuf>>>,
) {
    thread::spawn(move || {
        loop {
            if let Ok(mut stream) = TcpStream::connect(&listen_signal_server) {
                if stream.write_all(b"SUBSCRIBE\n").is_ok() {
                    let reader = BufReader::new(stream);
                    for line in reader.lines() {
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
                                
                                let is_dirty = listen_dirty.lock().unwrap().contains(&remote);
                                let is_ignored = listen_ignores.lock().unwrap().remove(&remote);
                                
                                if !is_dirty && !is_ignored {
                                    println!("[INVALIDATE] Server mutated file {:?}. Purging cache...", remote);
                                    let _ = fs::remove_file(&local_path);
                                    let _ = fs::remove_file(&base_path);
                                } else {
                                    println!("[INVALIDATE] Ignored self-triggered invalidation for {:?}", remote);
                                }
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
}
