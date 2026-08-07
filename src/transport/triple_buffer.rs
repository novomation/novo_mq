use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

const NEW_DATA_FLAG: usize = 0b100;
const INDEX_MASK: usize = 0b011;

#[derive(Debug)]
pub struct Shared<T: Sized> {
    // Each slot is individually cache-line padded. Without this, small `T`s
    // (e.g. `[u8; N]` payloads) pack multiple slots into the same cache
    // line; since the writer and reader almost always touch *different*
    // slots concurrently, that causes false sharing (needless cross-core
    // cache-line invalidation) on every write/read.
    mem: [CachePadded<UnsafeCell<Option<T>>>; 3],
    latest_free: CachePadded<AtomicUsize>,
}

impl<T> Shared<T> {
    fn new() -> Self {
        Shared {
            mem: [
                CachePadded::new(UnsafeCell::new(None)),
                CachePadded::new(UnsafeCell::new(None)),
                CachePadded::new(UnsafeCell::new(None)),
            ],
            latest_free: CachePadded::new(0.into()),
        }
    }
}

/// Number of bytes of (shared) memory required to back a triple buffer of `T`.
/// Use this to size the region (e.g. the `mmap`ped file) passed to [`triple_buffer`].
pub fn required_size<T>() -> usize {
    std::mem::size_of::<Shared<T>>()
}

/// Required alignment (in bytes) of the memory region passed to [`triple_buffer`].
///
/// `mmap`-backed regions (page-aligned) always satisfy this; a plain byte buffer
/// (e.g. `Vec<u8>`) generally does not and must be allocated with this alignment
/// explicitly (e.g. via [`std::alloc::alloc`] and [`std::alloc::Layout`]).
pub fn required_align<T>() -> usize {
    std::mem::align_of::<Shared<T>>()
}

/// Create a new wait-free triple buffer backed by the memory at `mem`.
///
/// # Safety
/// - `mem` must be valid for reads and writes for [`required_size::<T>()`](required_size)
///   bytes and aligned to [`required_align::<T>()`](required_align).
/// - `mem` must remain valid for as long as the returned [`Writer`]/[`Reader`] are used.
/// - This initializes the memory at `mem`; it must not already contain a live `Shared<T>`
///   (e.g. don't call this twice on the same region without the previous `Writer`/`Reader`
///   pair having been discarded).
pub unsafe fn triple_buffer<T>(mem: *mut u8) -> (Writer<T>, Reader<T>) {
    let shared = mem as *mut Shared<T>;
    unsafe {
        shared.write(Shared::new());
    }
    let w = Writer::new(shared);
    let r = Reader::new(shared);
    (w, r)
}

/// The reading side of the [triple_buffer].
#[derive(Debug)]
pub struct Reader<T> {
    shared: *mut Shared<T>,
    read_idx: usize,
}
unsafe impl<T: Send> Send for Reader<T> {}
unsafe impl<T: Send> Sync for Reader<T> {}

impl<T> Reader<T> {
    pub fn new(raw_mem: *mut Shared<T>) -> Self {
        Reader {
            shared: raw_mem,
            read_idx: 1,
        }
    }

    /// Reads the latest available value.
    /// Returns [None] if the [Writer] has not written anything yet.
    #[inline]
    pub fn try_read(&mut self) -> Option<T>
    where
        T: Clone,
    {
        let has_new_data =
            unsafe { (*self.shared).latest_free.load(Ordering::Acquire) } & NEW_DATA_FLAG > 0;
        if has_new_data {
            self.read_idx = unsafe {
                (*self.shared)
                    .latest_free
                    .swap(self.read_idx, Ordering::AcqRel)
                    & INDEX_MASK
            };
        }

        unsafe { (*(*self.shared).mem[self.read_idx].get()).clone() }
    }

    /// Reads the latest available value in place via `read`, without
    /// cloning it onto the stack first.
    ///
    /// `read` is called with a `&T` pointing directly at the latest slot in
    /// shared memory; its return value `R` is passed back. Returns [None]
    /// if the [Writer] has not written anything yet.
    #[inline]
    pub fn try_read_with<F, R>(&mut self, read: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        let has_new_data =
            unsafe { (*self.shared).latest_free.load(Ordering::Acquire) } & NEW_DATA_FLAG > 0;
        if has_new_data {
            self.read_idx = unsafe {
                (*self.shared)
                    .latest_free
                    .swap(self.read_idx, Ordering::AcqRel)
                    & INDEX_MASK
            };
        }

        let value = unsafe { (*(*self.shared).mem[self.read_idx].get()).as_ref() }?;
        Some(read(value))
    }
}

/// The writing side of the [triple_buffer].
#[derive(Debug)]
pub struct Writer<T> {
    shared: *mut Shared<T>,
    write_idx: usize,
    last_written: Option<usize>,
}
unsafe impl<T: Send> Send for Writer<T> {}
unsafe impl<T: Send> Sync for Writer<T> {}

pub trait ByteInit {
    fn init() -> Self;
}

impl<const N: usize> ByteInit for [u8; N] {
    fn init() -> Self {
        [0; N]
    }
}

impl<T> Writer<T> {
    pub fn new(raw_mem: *mut Shared<T>) -> Self {
        Writer {
            shared: raw_mem,
            write_idx: 2,
            last_written: None,
        }
    }

    /// Peeks at the last value written by this [Writer] (without consuming it).
    /// Returns [None] if nothing has been written yet.
    #[inline]
    pub fn try_read(&mut self) -> Option<T>
    where
        T: Clone,
    {
        let last_written = self.last_written?;

        unsafe { (*(*self.shared).mem[last_written].get()).clone() }
    }

    /// Writes an new value.
    #[inline]
    pub fn write(&mut self, data: T) {
        // Drop old value and write new one
        unsafe {
            (*(*self.shared).mem[self.write_idx & INDEX_MASK].get()) = Some(data);
        }

        // Store index
        self.last_written = Some(self.write_idx & INDEX_MASK);
        self.write_idx = unsafe {
            (*self.shared)
                .latest_free
                .swap(self.write_idx | NEW_DATA_FLAG, Ordering::AcqRel)
        };
    }

    /// Writes a new value in place via `init`, without going through an
    /// intermediate stack copy.
    ///
    /// `init` is called with a `&mut T` pointing directly at the next free
    /// slot in shared memory (pre-populated with `T::default()` if it was
    /// previously empty, otherwise still holding whatever value was written
    /// there two writes ago). This lets callers (e.g. copy bytes into a
    /// fixed-size buffer) write directly into shared memory instead of
    /// building the value on the stack and then moving/copying it in via
    /// [`Writer::write`].
    #[inline]
    pub fn write_with<F>(&mut self, init: F)
    where
        T: ByteInit,
        F: FnOnce(&mut T),
    {
        let idx = self.write_idx & INDEX_MASK;
        unsafe {
            let slot = (*self.shared).mem[idx].get();
            if (*slot).is_none() {
                *slot = Some(T::init());
            }
            // Safety: we just ensured `*slot` is `Some(_)`.
            init((*slot).as_mut().unwrap_unchecked());
        }

        // Store index
        self.last_written = Some(idx);
        self.write_idx = unsafe {
            (*self.shared)
                .latest_free
                .swap(self.write_idx | NEW_DATA_FLAG, Ordering::AcqRel)
        };
    }
}

impl<T: ByteInit> ByteInit for (usize, T) {
    fn init() -> Self {
        (std::mem::size_of::<T>(), T::init())
    }
}

impl<T: AsMut<[u8]> + ByteInit> Writer<(usize, T)> {
    /// Copies `buf` directly into the next shared-memory slot as raw bytes
    /// (truncated to the slot's capacity), and publishes it as the newest
    /// value.
    ///
    /// Returns the number of bytes actually copied; if this is less than
    /// `buf.len()`, the slot's capacity was too small and the data was
    /// truncated.
    #[inline]
    pub fn write_bytes(&mut self, buf: &[u8]) -> usize {
        let mut copied = 0;
        self.write_with(|(len, data)| {
            let dst = data.as_mut();
            copied = buf.len().min(dst.len());
            dst[..copied].copy_from_slice(&buf[..copied]);
            *len = copied;
        });
        copied
    }
}

impl<T: AsRef<[u8]>> Reader<(usize, T)> {
    /// Copies the latest available message directly out of shared memory
    /// into `buf`, without cloning the full slot value onto the stack first.
    ///
    /// Returns `Some(msg_len)` with the original message length (which may
    /// be larger than `buf.len()`, in which case only `buf.len()` bytes were
    /// copied into `buf` and the caller should treat this as truncation).
    /// Returns `None` if the [Writer] has not written anything yet.
    #[inline]
    pub fn read_bytes(&mut self, buf: &mut [u8]) -> Option<usize> {
        self.try_read_with(|(msg_len, data)| {
            let src = &data.as_ref()[..*msg_len];
            let copied = buf.len().min(*msg_len);
            buf[..copied].copy_from_slice(&src[..copied]);
            *msg_len
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::alloc::{Layout, alloc};

    /// Test helper: backs a triple buffer with a correctly-aligned heap
    /// allocation and leaks it so the memory outlives the returned
    /// `Writer`/`Reader`.
    fn new_buffer<T>() -> (Writer<T>, Reader<T>) {
        let layout = Layout::from_size_align(required_size::<T>(), required_align::<T>()).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "allocation failed");
        unsafe { triple_buffer(ptr) }
    }

    #[test]
    fn smoke() {
        let (mut w, mut r) = new_buffer();
        w.write([0; 15]);
        assert_eq!(w.try_read(), Some([0; 15]));
        assert_eq!(r.try_read(), Some([0; 15]));
    }

    #[test]
    fn test_read_none() {
        let (mut w, mut r) = new_buffer::<Vec<i32>>();
        assert_eq!(r.try_read(), None);
        w.write(vec![0; 15]);
        assert_eq!(r.try_read(), Some(vec![0; 15]));
    }
}
