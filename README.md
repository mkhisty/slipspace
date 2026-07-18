# Slipspace

A high-performance remote workspace syncer designed to feel as fast as local development. 

Currently, when using an AI agent for remote development, users have to either:
1. Download the entire AI agent setup on the remote machine.
2. Use skills to send commands to the server through SSH - forcing the AI to use inefficient CLI tools compared to the custom tooling it ships with.
3. Use remote file mounting which forces the user to:
    - Endure heavy latency.
    - Or use a caching system and risk actions on the remote using outdated files.

Slipspace solves this by using a cached virtual directory which almost instantly syncs with the remote, yet locks any operations using those files if the operation is triggered before files are completely synced.

## How it works

1. **Client FUSE:** Mounts the remote workspace locally. Reads are instant via local caching. Writes are instantly saved locally and synced in the background.
2. **Server Daemon:** A FUSE interceptor that transparently sits over the remote folder to manage conflicts.
3. **Skills:** A toolset that allows the AI to efficiently use Slipspace.

## Status

Currently split into two binary components for MVP testing:
* `server-daemon` (Server interceptor and patch applier)
* `sshfs-rs` (Client FUSE mount and background sync)
