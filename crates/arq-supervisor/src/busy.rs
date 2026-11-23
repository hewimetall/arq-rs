//! One delivery counter per slot, on a shared memfd page.
//!
//! A non-zero digit means the slot is busy. A soft memory signal skips that child.
//! The parent stores zero before the next spawn so a `SIGKILL` does not stick.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

const MFD_CLOEXEC: libc::c_uint = 0x0001;
const MFD_ALLOW_SEALING: libc::c_uint = 0x0002;

pub const SLOT_FD_ENV: &str = "ARQRS_SLOT_FD";
pub const SLOT_INDEX_ENV: &str = "ARQRS_SLOT";

pub struct SlotDigits {
    fd: OwnedFd,
    map: *mut u8,
    len: usize,
    slots: u32,
}

unsafe impl Send for SlotDigits {}
unsafe impl Sync for SlotDigits {}

impl SlotDigits {
    pub fn create(slots: u32) -> io::Result<Self> {
        let page = page_size();
        let bytes = (slots as usize).saturating_mul(std::mem::size_of::<AtomicU32>()).max(std::mem::size_of::<AtomicU32>());
        let len = bytes.div_ceil(page) * page;
        let rc = unsafe { libc::memfd_create(c"arq-slot".as_ptr(), MFD_CLOEXEC | MFD_ALLOW_SEALING) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = unsafe { OwnedFd::from_raw_fd(rc) };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let map = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd.as_raw_fd(), 0)
        };
        if map == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let seals = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, seals) } != 0 {
            unsafe { libc::munmap(map, len) };
            return Err(io::Error::last_os_error());
        }
        let digits = Self { fd, map: map.cast(), len, slots };
        for slot in 0..slots {
            digits.reset(slot);
        }
        Ok(digits)
    }

    pub fn raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }

    pub fn reset(&self, slot: u32) {
        self.with_cell(slot, |cell| cell.store(0, Ordering::Relaxed));
    }

    pub fn is_busy(&self, slot: u32) -> bool {
        self.load(slot) != 0
    }

    pub fn enter(&self, slot: u32) -> bool {
        self.with_cell(slot, |cell| {
            cell.fetch_add(1, Ordering::Relaxed);
        })
        .is_some()
    }

    pub fn leave(&self, slot: u32) {
        self.with_cell(slot, |cell| {
            cell.fetch_sub(1, Ordering::Relaxed);
        });
    }

    fn load(&self, slot: u32) -> u32 {
        self.with_cell(slot, |cell| cell.load(Ordering::Relaxed)).unwrap_or(0)
    }

    fn with_cell<T>(&self, slot: u32, f: impl FnOnce(&AtomicU32) -> T) -> Option<T> {
        if slot >= self.slots {
            return None;
        }
        let ptr = unsafe { (self.map as *mut u32).add(slot as usize) };
        if !ptr.is_aligned() {
            return None;
        }
        Some(f(unsafe { AtomicU32::from_ptr(ptr) }))
    }
}

impl Drop for SlotDigits {
    fn drop(&mut self) {
        unsafe { libc::munmap(self.map.cast(), self.len) };
    }
}

pub fn open_slot_digits(slots: u32) -> io::Result<Arc<SlotDigits>> {
    Ok(Arc::new(SlotDigits::create(slots)?))
}

fn page_size() -> usize {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page > 0 { page as usize } else { 4096 }
}

static CHILD_DIGIT: OnceLock<&'static AtomicU32> = OnceLock::new();

/// Map the inherited slot page. Absent env means this process is not a supervised slot.
pub fn install_slot_digit() -> io::Result<()> {
    let Some(raw) = std::env::var(SLOT_FD_ENV).ok() else {
        return Ok(());
    };
    let fd: i32 = raw.parse().map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "slot fd"))?;
    let slot: u32 = std::env::var(SLOT_INDEX_ENV)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "slot index"))?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "slot index"))?;
    let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
    if unsafe { libc::fstat(fd, &mut stat) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let len = usize::try_from(stat.st_size).unwrap_or(0);
    let offset = (slot as usize).saturating_mul(std::mem::size_of::<AtomicU32>());
    if offset.saturating_add(std::mem::size_of::<AtomicU32>()) > len {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "slot is outside the digit page"));
    }
    let map = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd, 0) };
    if map == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    unsafe { libc::close(fd) };
    unsafe { std::env::remove_var(SLOT_FD_ENV) };
    let ptr = unsafe { (map as *mut u32).add(slot as usize) };
    if !ptr.is_aligned() {
        unsafe { libc::munmap(map, len) };
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "slot digit is not aligned"));
    }
    let cell: &'static AtomicU32 = unsafe { AtomicU32::from_ptr(ptr) };
    if CHILD_DIGIT.set(cell).is_err() {
        unsafe { libc::munmap(map, len) };
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, "slot digit installed twice"));
    }
    Ok(())
}

pub struct BusyGuard {
    cell: &'static AtomicU32,
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.cell.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn enter_delivery() -> Option<BusyGuard> {
    let cell = *CHILD_DIGIT.get()?;
    cell.fetch_add(1, Ordering::Relaxed);
    Some(BusyGuard { cell })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        crate::testutil::env_lock()
    }

    #[test]
    fn a_busy_digit_is_visible_to_the_parent_and_reset_before_reuse() {
        let digits = SlotDigits::create(2).unwrap();
        assert!(!digits.is_busy(0));
        assert!(digits.enter(0));
        assert!(digits.is_busy(0));
        assert!(!digits.is_busy(1));
        assert!(!digits.enter(9));
        digits.leave(0);
        assert!(!digits.is_busy(0));
        digits.enter(1);
        digits.reset(1);
        assert!(!digits.is_busy(1));
        assert!(!digits.is_busy(4));
        let _ = digits.raw_fd();
    }

    #[test]
    fn the_child_maps_the_inherited_page_before_any_python_import() {
        let _guard = env_lock();
        let digits = open_slot_digits(1).unwrap();
        digits.reset(0);
        let dup = unsafe { libc::dup(digits.raw_fd()) };
        assert!(dup >= 0);
        unsafe { std::env::set_var(SLOT_FD_ENV, dup.to_string()) };
        unsafe { std::env::set_var(SLOT_INDEX_ENV, "0") };
        install_slot_digit().unwrap();
        assert!(std::env::var(SLOT_FD_ENV).is_err());
        let guard = enter_delivery().expect("digit");
        assert!(digits.is_busy(0));
        drop(guard);
        assert!(!digits.is_busy(0));
        let again = unsafe { libc::dup(digits.raw_fd()) };
        unsafe { std::env::set_var(SLOT_FD_ENV, again.to_string()) };
        unsafe { std::env::set_var(SLOT_INDEX_ENV, "0") };
        assert!(install_slot_digit().is_err());
        unsafe { std::env::remove_var(SLOT_FD_ENV) };
        unsafe { std::env::remove_var(SLOT_INDEX_ENV) };
    }

    #[test]
    fn a_missing_slot_env_is_not_a_supervised_child() {
        let _guard = env_lock();
        unsafe { std::env::remove_var(SLOT_FD_ENV) };
        install_slot_digit().unwrap();
    }

    #[test]
    fn a_bad_slot_descriptor_is_rejected() {
        let _guard = env_lock();
        unsafe { std::env::set_var(SLOT_FD_ENV, "nope") };
        unsafe { std::env::set_var(SLOT_INDEX_ENV, "0") };
        assert!(install_slot_digit().is_err());
        unsafe { std::env::set_var(SLOT_FD_ENV, "3") };
        unsafe { std::env::remove_var(SLOT_INDEX_ENV) };
        assert!(install_slot_digit().is_err());
        unsafe { std::env::set_var(SLOT_FD_ENV, "3") };
        unsafe { std::env::set_var(SLOT_INDEX_ENV, "nope") };
        assert!(install_slot_digit().is_err());
        unsafe { std::env::set_var(SLOT_FD_ENV, "999999") };
        unsafe { std::env::set_var(SLOT_INDEX_ENV, "0") };
        assert!(install_slot_digit().is_err());
        let digits = SlotDigits::create(1).unwrap();
        let dup = unsafe { libc::dup(digits.raw_fd()) };
        assert!(dup >= 0);
        unsafe { std::env::set_var(SLOT_FD_ENV, dup.to_string()) };
        unsafe { std::env::set_var(SLOT_INDEX_ENV, "100000") };
        assert!(install_slot_digit().is_err());
        unsafe { libc::close(dup) };
        unsafe { std::env::remove_var(SLOT_FD_ENV) };
        unsafe { std::env::remove_var(SLOT_INDEX_ENV) };
    }
}
