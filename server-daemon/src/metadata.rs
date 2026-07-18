use fuser::{FileAttr, FileType};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::time::{Duration, UNIX_EPOCH};

pub const TTL: Duration = Duration::from_secs(0);

pub fn fs_meta_to_attr(ino: u64, meta: &fs::Metadata) -> FileAttr {
    let kind = if meta.is_dir() {
        FileType::Directory
    } else if meta.file_type().is_symlink() {
        FileType::Symlink
    } else {
        FileType::RegularFile
    };

    FileAttr {
        ino,
        size: meta.size(),
        blocks: meta.blocks(),
        atime: UNIX_EPOCH + Duration::from_secs(meta.atime() as u64),
        mtime: UNIX_EPOCH + Duration::from_secs(meta.mtime() as u64),
        ctime: UNIX_EPOCH + Duration::from_secs(meta.ctime() as u64),
        crtime: UNIX_EPOCH + Duration::from_secs(meta.ctime() as u64),
        kind,
        perm: (meta.mode() & 0o777) as u16,
        nlink: meta.nlink() as u32,
        uid: meta.uid(),
        gid: meta.gid(),
        rdev: meta.rdev() as u32,
        blksize: meta.blksize() as u32,
        flags: 0,
    }
}
