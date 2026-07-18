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
use std::thread;
use std::time::Duration;

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

    /// Automatically download and start the server daemon if it is not reachable
    #[arg(long, action = clap::ArgAction::SetTrue)]
    auto_install: bool,

    /// URL of the pre‑built server‑daemon binary (GitHub release). If omitted a default is used.
    #[arg(long)]
    daemon_url: Option<String>,
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

    let signal_parts: Vec<&str> = args.signal_server.split(':').collect();
    let remote_signal_port = if signal_parts.len() == 2 { signal_parts[1] } else { "8080" };
    let local_signal_port = remote_signal_port;
    let local_signal_server = format!("127.0.0.1:{}", local_signal_port);

    println!("Opening secure SSH tunnel for signaling (Local {} -> Remote {})...", local_signal_port, remote_signal_port);
    let ssh_args = vec![
        "-N".to_string(),
        "-L".to_string(),
        format!("{}:127.0.0.1:{}", local_signal_port, remote_signal_port),
        format!("{}@{}", user, host),
        "-p".to_string(),
        port.to_string(),
    ];
    let mut tunnel_cmd = if let Some(ref pw) = password {
        let mut cmd = std::process::Command::new("sshpass");
        cmd.arg("-p").arg(pw).arg("ssh").args(&ssh_args);
        cmd
    } else {
        let mut cmd = std::process::Command::new("ssh");
        cmd.args(&ssh_args);
        cmd
    };
    let ssh_tunnel = tunnel_cmd.spawn().expect("Failed to start SSH tunnel. If using password auth, ensure 'sshpass' is installed.");
    let tunnel_child = Arc::new(Mutex::new(ssh_tunnel));
    thread::sleep(Duration::from_secs(1));

    // ---------------------------------------------------------------------
    // Auto‑install the server daemon if needed (only when the flag is set)
    // ---------------------------------------------------------------------
    if args.auto_install {
        // First, try a quick connection to the signal server
        let daemon_ready = TcpStream::connect(&args.signal_server).is_ok();
        if !daemon_ready {
            // Build the download URL – use the user supplied one or a default.
            let daemon_url = args.daemon_url.clone().unwrap_or_else(|| {
                // Default GitHub release URL – replace with your actual repo/tag pattern.
                "https://github.com/mkhisty/slipspace/releases/latest/download/server-daemon-musl".to_string()
            });

            println!("Daemon not reachable – downloading from {}...", daemon_url);
            // Remote command: download, make executable, launch in background.
            let remote_cmd = format!(
                "curl -L -o /tmp/slipspace_server_daemon '{}' && chmod +x /tmp/slipspace_server_daemon && nohup /tmp/slipspace_server_daemon --path '{}' > /tmp/slipspace_server.log 2>&1 &",
                daemon_url,
                remote_path
            );
            let mut channel = sess.channel_session().expect("Failed to open SSH channel");
            channel.exec(&remote_cmd).expect("Failed to execute remote install command");
            let _ = channel.wait_close();
            println!("Remote daemon install command issued; waiting for it to start...");
            // Simple retry loop – try up to 5 times, 1 s apart.
            for _ in 0..5 {
                if TcpStream::connect(&local_signal_server).is_ok() {
                    break;
                }
                thread::sleep(Duration::from_secs(1));
            }
        }
    }

    println!("Opening SFTP session...");
    let sftp = sess.sftp().unwrap();

    let (tx, rx) = unbounded::<(PathBuf, PathBuf)>();
    let dirty_files = Arc::new(Mutex::new(HashSet::new()));
    let versions = Arc::new(Mutex::new(HashMap::new()));
    let pending_ignores = Arc::new(Mutex::new(HashSet::new()));

    run_invalidation_listener(
        local_signal_server.clone(),
        PathBuf::from(&args.cache_dir),
        PathBuf::from(&remote_path),
        Arc::clone(&dirty_files),
        Arc::clone(&pending_ignores),
    );

    run_sync_thread(
        rx,
        local_signal_server.clone(),
        Arc::clone(&versions),
        Arc::clone(&dirty_files),
        args.test_delay,
        PathBuf::from(remote_path.clone()),
    );

    let fs = SshFs::new(
        sftp, 
        remote_path, 
        PathBuf::from(args.cache_dir.clone()), 
        local_signal_server.clone(), 
        tx, 
        dirty_files, 
        versions,
        pending_ignores
    );

    let options = vec![
        MountOption::FSName("slipspace".to_string()),
    ];

    let mnt_clone = mount_point.clone();
    let sig_server = local_signal_server.clone();
    let cache_dir_clone = args.cache_dir.clone();
    let tunnel_clone = Arc::clone(&tunnel_child);
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl+C! Sending SHUTDOWN to server...");
        if let Ok(mut stream) = std::net::TcpStream::connect(&sig_server) {
            use std::io::Write;
            let _ = stream.write_all(b"SHUTDOWN\n");
        }
        
        println!("Unmounting FUSE and cleaning up directories...");
        let mnt_path = std::path::Path::new(&mnt_clone);
        let _ = std::process::Command::new("fusermount3").arg("-u").arg("-z").arg(mnt_path).output();
        
        let _ = std::fs::remove_dir_all(mnt_path);
        let _ = std::fs::remove_dir_all(std::path::Path::new(&cache_dir_clone));

        if let Ok(mut child) = tunnel_clone.lock() {
            let _ = child.kill();
        }

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
