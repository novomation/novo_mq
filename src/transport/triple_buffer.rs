use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

const NEW_DATA_FLAG: usize = 0b100;
const INDEX_MASK: usize = 0b011;

#[derive(Debug)]
pub struct Shared<T: Sized> {
    mem: [UnsafeCell<Option<T>>; 3],
    latest_free: CachePadded<AtomicUsize>,
}

impl<T> Shared<T> {
    fn new() -> Self {
        Shared {
            mem: [
                UnsafeCell::new(None),
                UnsafeCell::new(None),
                UnsafeCell::new(None),
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
