mod cache;
mod fs;
mod metadata;
mod signal;
mod sync;

use clap::Parser;
use crossbeam_channel::unbounded;
use fuser::MountOption;
use ssh2::Session;
use std::collections::{HashMap, HashSet};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::fs::SshFs;
use crate::signal::run_invalidation_listener;
use crate::sync::run_sync_thread;

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
    let pending_ignores = Arc::new(Mutex::new(HashSet::new()));

    run_invalidation_listener(
        args.signal_server.clone(),
        PathBuf::from(&args.cache_dir),
        PathBuf::from(&remote_path),
        Arc::clone(&dirty_files),
        Arc::clone(&pending_ignores),
    );

    run_sync_thread(
        rx,
        args.signal_server.clone(),
        Arc::clone(&versions),
        Arc::clone(&dirty_files),
        args.test_delay,
        PathBuf::from(remote_path.clone()),
    );

    let fs = SshFs::new(
        sftp, 
        remote_path, 
        PathBuf::from(args.cache_dir), 
        args.signal_server, 
        tx, 
        dirty_files, 
        versions,
        pending_ignores
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
