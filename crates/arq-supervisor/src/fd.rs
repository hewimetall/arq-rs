//! Descriptor flags used when a child inherits the parent pidfd or the slot page.

use std::io;

pub fn fd_flags(fd: i32) -> io::Result<i32> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 { Err(io::Error::last_os_error()) } else { Ok(flags) }
}

pub fn status_flags(fd: i32) -> io::Result<i32> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 { Err(io::Error::last_os_error()) } else { Ok(flags) }
}

pub fn clear_cloexec(fd: i32) -> io::Result<()> {
    let flags = fd_flags(fd)?;
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn set_cloexec(fd: i32) -> io::Result<()> {
    let flags = fd_flags(fd)?;
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn set_nonblock(fd: i32) -> io::Result<()> {
    let flags = status_flags(fd)?;
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn is_cloexec(fd: i32) -> io::Result<bool> {
    Ok(fd_flags(fd)? & libc::FD_CLOEXEC != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::{FromRawFd, OwnedFd};

    fn memfd() -> OwnedFd {
        let rc = unsafe { libc::memfd_create(c"arq-fd".as_ptr(), 0) };
        assert!(rc >= 0);
        unsafe { OwnedFd::from_raw_fd(rc) }
    }

    #[test]
    fn cloexec_and_nonblock_round_trip() {
        let fd = memfd();
        let raw = std::os::fd::AsRawFd::as_raw_fd(&fd);
        clear_cloexec(raw).unwrap();
        assert!(!is_cloexec(raw).unwrap());
        set_cloexec(raw).unwrap();
        assert!(is_cloexec(raw).unwrap());
        set_nonblock(raw).unwrap();
        assert!(status_flags(raw).unwrap() & libc::O_NONBLOCK != 0);
        assert!(fd_flags(-1).is_err());
        assert!(status_flags(-1).is_err());
        assert!(clear_cloexec(-1).is_err());
        assert!(set_cloexec(-1).is_err());
        assert!(set_nonblock(-1).is_err());
        assert!(is_cloexec(-1).is_err());
    }
}
