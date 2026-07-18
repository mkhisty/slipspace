use crossbeam_channel::Receiver;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub fn run_sync_thread(
    rx: Receiver<(PathBuf, PathBuf)>,
    signal_server: String,
    bg_versions: Arc<Mutex<HashMap<PathBuf, u64>>>,
    bg_dirty: Arc<Mutex<HashSet<PathBuf>>>,
    test_delay: bool,
    bg_remote_root: PathBuf,
) {
    thread::spawn(move || {
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

            println!("Background sync starting for {:?}", remote_path);

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
}
