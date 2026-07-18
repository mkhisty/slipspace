# Slipspace

A high-performance remote workspace syncer designed to feel as fast as local development. 


Currently, when using an AI agent for remote developement, users have to either
a. Download entire ai agent setup on remote machine
b. Use skills to send commands to server through ssh - forcing ai to use inefficient cli tools compared to custom tooling it ships with.
c. Remote file mounting which forces user to 
    a. endure heavy latency 
    b. or use caching system and risk actions on remote using outdated files.

Slipspace solves this by using a cached virtual directory which almost instantly syncs w/ remote, yet locks any operations using those files if the operation is triggered before files are completely synced.


## How it works

1. **Client FUSE:** Mounts the remote workspace locally. Reads are instant via local caching. Writes are instantly saved locally and synced in the background.
2. **Server Daemon:** A FUSE interceptor that transparently sits over the remote folder to manage conflicts.
3. **Skills** A toolset that allows AI to efficiently use Slipspace.
## Status

Currently split into two binary components for MVP testing:
* `server-daemon` (Server interceptor and patch applier)
* `sshfs-rs` (Client FUSE mount and background sync)

