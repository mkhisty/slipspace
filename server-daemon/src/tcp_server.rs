use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub fn start_tcp_listener(
    dirty_files: Arc<Mutex<HashSet<PathBuf>>>,
    notify: Arc<Condvar>,
    backing_dir: PathBuf,
    subscribers: Arc<Mutex<Vec<std::net::TcpStream>>>,
) {
    thread::spawn(move || {
        let listener = TcpListener::bind("0.0.0.0:8080").expect("Failed to bind port 8080");
        println!("Server daemon listening for signals on port 8080...");
        
        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let df = Arc::clone(&dirty_files);
                let notif = Arc::clone(&notify);
                let bg_backing = backing_dir.clone();
                let subs_clone = Arc::clone(&subscribers);
                
                thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut client_locks = HashSet::new();
                    let mut line_buf = String::new();
                    
                    loop {
                        line_buf.clear();
                        if BufRead::read_line(&mut reader, &mut line_buf).unwrap_or(0) == 0 {
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
                                        if Read::read_exact(&mut reader, &mut patch_data).is_ok() {
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
}
