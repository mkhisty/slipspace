mod fs;
mod metadata;
mod tcp_server;

use clap::Parser;
use fuser::MountOption;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs as std_fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use crate::fs::ServerDaemonFs;
use crate::tcp_server::start_tcp_listener;

#[derive(Parser, Debug)]
#[command(name = "server-daemon", author, version, about = "Slipspace Server Daemon", long_about = None)]
struct Args {
    /// The path to the directory that should be intercepted by FUSE
    #[arg(short, long)]
    path: String,
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
        let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(&target).output();
        let _ = std_fs::remove_dir_all(&target);
        std_fs::create_dir_all(&target).unwrap();
    } else {
        if !target.exists() {
            std_fs::create_dir_all(&backing).unwrap();
            let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(&target).output();
            let _ = std_fs::remove_dir_all(&target);
            std_fs::create_dir_all(&target).unwrap();
        } else {
            std_fs::rename(&target, &backing).expect("Failed to move target directory to backing store");
            let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(&target).output();
            let _ = std_fs::remove_dir_all(&target);
            std_fs::create_dir_all(&target).unwrap();
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

    start_tcp_listener(df_clone, notify_clone, backing_clone, subscribers_clone);

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
