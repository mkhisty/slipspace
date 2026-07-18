use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

pub fn start_tcp_listener(
    dirty_files: Arc<Mutex<HashSet<PathBuf>>>,
    notify: Arc<Condvar>,
    target_dir: PathBuf,
    backing_dir: PathBuf,
    subscribers: Arc<Mutex<Vec<std::net::TcpStream>>>,
    port: u16,
) {
    thread::spawn(move || {
        let bind_addr = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&bind_addr).unwrap_or_else(|_| panic!("Failed to bind to {}", bind_addr));
        println!("Server daemon listening securely for signals on {}...", bind_addr);
        
        let active_connections = Arc::new(Mutex::new(0));
        
        // Spawn Watchdog Thread
        let watchdog_active = Arc::clone(&active_connections);
        let watchdog_target = target_dir.clone();
        let watchdog_backing = backing_dir.clone();
        thread::spawn(move || {
            let mut seconds_without_clients = 0;
            loop {
                thread::sleep(std::time::Duration::from_secs(10));
                let count = *watchdog_active.lock().unwrap();
                if count == 0 {
                    seconds_without_clients += 10;
                    if seconds_without_clients >= 60 {
                        println!("[WATCHDOG] No clients connected for 60 seconds! Shutting down zombie server...");
                        crate::restore_workspace(&watchdog_target, &watchdog_backing);
                        std::process::exit(0);
                    }
                } else {
                    seconds_without_clients = 0;
                }
            }
        });

        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let df = Arc::clone(&dirty_files);
                let notif = Arc::clone(&notify);
                let bg_target = target_dir.clone();
                let bg_backing = backing_dir.clone();
                let subs_clone = Arc::clone(&subscribers);
                let conn_count = Arc::clone(&active_connections);
                
                thread::spawn(move || {
                    {
                        let mut c = conn_count.lock().unwrap();
                        *c += 1;
                        println!("[SIGNAL] Client connected. Total active connections: {}", *c);
                    }
                    
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
                            } else if cmd == "SHUTDOWN" {
                                println!("[SIGNAL] Received SHUTDOWN from client. Exiting...");
                                crate::restore_workspace(&bg_target, &bg_backing);
                                std::process::exit(0);
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
                    
                    {
                        let mut c = conn_count.lock().unwrap();
                        *c -= 1;
                        println!("[SIGNAL] Client disconnected. Total active connections: {}", *c);
                    }
                });
            }
        }
    });
}
