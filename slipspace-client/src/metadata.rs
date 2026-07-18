use fuser::{FileAttr, FileType};
use std::time::{Duration, UNIX_EPOCH};

pub const TTL: Duration = Duration::from_secs(0);

pub fn sftp_stat_to_attr(ino: u64, stat: &ssh2::FileStat) -> FileAttr {
    let kind = if stat.is_dir() {
        FileType::Directory
    } else {
        FileType::RegularFile
    };

    let mtime = stat.mtime.unwrap_or(0);
    let atime = stat.atime.unwrap_or(0);

    FileAttr {
        ino,
        size: stat.size.unwrap_or(0),
        blocks: (stat.size.unwrap_or(0) + 511) / 512,
        atime: UNIX_EPOCH + Duration::from_secs(atime),
        mtime: UNIX_EPOCH + Duration::from_secs(mtime),
        ctime: UNIX_EPOCH + Duration::from_secs(mtime),
        crtime: UNIX_EPOCH + Duration::from_secs(mtime),
        kind,
        perm: stat.perm.unwrap_or(0o755) as u16,
        nlink: 1,
        uid: stat.uid.unwrap_or(0),
        gid: stat.gid.unwrap_or(0),
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}
