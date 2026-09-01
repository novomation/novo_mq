//! A wait-free single-producer single-consumer (SPSC) queue for sending
//! messages between threads — and, because it lives entirely in (shared)
//! memory, across process boundaries via the `SpscTransport`.
//!
//! Unlike [`std::sync::mpsc`], the queue has a fixed capacity, so
//! [`Sender::try_send`] and [`Receiver::try_recv`] never block and never
//! allocate: they return immediately with a [`SendError`] / [`TryRecvError`]
//! instead of parking the calling thread.
//!
//! # Shared-memory constructibility
//!
//! The queue state ([`Shared<T, N>`]) is a plain struct that can be placed in
//! any memory region it has valid access to — see [`spsc_in_memory`],
//! [`required_size`], [`required_align`] — exactly like the triple buffer in
//! [`crate::transport::algos::triple_buffer`]. This is what lets the
//! [`SpscTransport`](crate::transport::spsc::SpscTransport) negotiate a queue
//! over a memfd + Unix socket.
//!
//! ```ignore
//! //                        Type ──╮  ╭─ Capacity
//! let (mut tx, mut rx) = spsc_in_memory::<u64, 8>(mem);
//! tx.try_send(234);
//! assert_eq!(rx.try_recv(), Ok(234u64));
//! ```
use core::error::Error;
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// The shared queue state: a fixed-capacity (power of two) ring of slots plus
/// monotonically-increasing (wrapping) read/write commit counters.
///
/// `N` is the number of usable slots and must be a power of two. Only the
/// *writer* may write `write`/the slot at the write position, and only the
/// *reader* may write `read`/clear a slot — there is a single producer and a
/// single consumer, which is what makes the queue wait-free.
///
/// Every field that is written independently by one side is padded onto its
/// own cache line so the producer and consumer don't thrash each other's
/// cache lines (false sharing).
#[repr(align(64))]
pub struct Shared<T, const N: usize> {
    mem: [Slot<T>; N],
    read: CachePadded<AtomicUsize>,
    write: CachePadded<AtomicUsize>,
}

impl<T, const N: usize> Shared<T, N> {
    /// Initialise a freshly-backed (zeroed or otherwise uninitialised) region
    /// of memory as an empty queue. See [`spsc_in_memory`].
    ///
    /// # Panics
    /// Panics (in debug builds) if `N` is not a power of two.
    #[allow(clippy::new_ret_no_self)]
    fn new(mem: *mut Self) {
        debug_assert!(
            is_power_of_two(N),
            "SPSC queue capacity {N} must be a power of two"
        );
        for slot in unsafe { (*mem).mem.iter_mut() } {
            slot.occupied.store(false, Ordering::Relaxed);
            unsafe {
                *slot.value.get() = None;
            }
        }
        unsafe {
            (*mem).read.store(0, Ordering::Relaxed);
            (*mem).write.store(0, Ordering::Relaxed);
        }
    }
}

/// Number of bytes of shared memory required to back an SPSC queue of `T`
/// with capacity `N`. Use this to size the region passed to
/// [`spsc_in_memory`] (e.g. the `mmap`ped memfd).
pub const fn required_size<T, const N: usize>() -> usize {
    std::mem::size_of::<Shared<T, N>>()
}

/// Required alignment of the shared-memory region passed to
/// [`spsc_in_memory`]. `mmap`-backed regions (page-aligned) always satisfy
/// this, but a plain `Vec<u8>` generally does not and must be allocated with
/// this alignment explicitly (see `std::alloc::Layout`).
#[allow(unused)]
pub const fn required_align<T, const N: usize>() -> usize {
    std::mem::align_of::<Shared<T, N>>()
}

const fn is_power_of_two(x: usize) -> bool {
    let c = x.wrapping_sub(1);
    (x != 0) && ((x & c) == 0)
}

/// Create a new wait-free SPSC queue backed by the shared memory at `mem`.
///
/// `N` is the capacity (a power of two) and both ends use the same `T`/`N`,
/// so the producer and consumer agree on the layout with no runtime
/// negotiation.
///
/// # Safety
/// - `mem` must be valid for reads and writes for [`required_size::<T, N>()`]
///   bytes and aligned to [`required_align::<T, N>()`].
/// - `mem` must remain valid for as long as the returned [`Sender`]/[`Receiver`]
///   are used.
/// - This initialises the memory at `mem`; it must not already contain a live
///   `Shared<T, N>` (do not call this twice on the same region without the
///   previous pair having been discarded).
pub unsafe fn spsc_in_memory<T, const N: usize>(mem: *mut u8) -> (Sender<T, N>, Receiver<T, N>) {
    let shared = mem as *mut Shared<T, N>;
    Shared::new(shared);
    (unsafe { Sender::new(shared) }, unsafe {
        Receiver::new(shared)
    })
}

/// A single slot of the ring buffer: an optional value plus an occupied flag.
#[derive(Debug)]
struct Slot<T> {
    value: UnsafeCell<Option<T>>,
    occupied: CachePadded<AtomicBool>,
}

/// The sending (producing) side of the queue.
#[derive(Debug)]
pub struct Sender<T, const N: usize> {
    shared: *mut Shared<T, N>,
}
unsafe impl<T, const N: usize> Send for Sender<T, N> where T: Send {}
unsafe impl<T, const N: usize> Sync for Sender<T, N> where T: Send {}

impl<T, const N: usize> Sender<T, N> {
    /// Wraps an already-initialised shared-memory region (see
    /// [`spsc_in_memory`]). `new` does not re-initialise the memory.
    ///
    /// # Safety
    /// `shared` must be a valid, initialised `Shared<T, N>` region
    /// (see [`spsc_in_memory`]).
    pub const unsafe fn new(shared: *mut Shared<T, N>) -> Self {
        Sender { shared }
    }

    /// Attempts to send a value into the queue without blocking.
    ///
    /// Returns [`SendError::NoSpaceLeft`] (with the value) if the queue is
    /// full, rather than blocking or growing the buffer.
    pub fn try_send(&mut self, data: T) -> Result<(), SendError<T>> {
        // Acquire: synchronises with a prior Release store of `occupied` from
        // the consumer freeing exactly this slot (wrap-around).
        let write = unsafe { (*self.shared).write.load(Ordering::Acquire) };
        let wpos = write & (N - 1);
        let slot = unsafe { (*self.shared).mem.get_unchecked(wpos) };
        if slot.occupied.load(Ordering::Acquire) {
            return Err(SendError::NoSpaceLeft(data));
        }
        unsafe {
            *slot.value.get() = Some(data);
        }
        // Release: the consumer's Acquire load of `occupied` synchronises with
        // this, making the value visible before it sees the flag.
        slot.occupied.store(true, Ordering::Release);
        // Release: publishes this slot to the consumer.
        unsafe {
            (*self.shared)
                .write
                .store(write.wrapping_add(1), Ordering::Release)
        };
        Ok(())
    }

    /// The queue's total capacity in items.
    #[allow(unused)]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Best-effort number of items currently queued (relaxed atomic read).
    #[allow(unused)]
    pub fn len(&self) -> usize {
        unsafe {
            (*self.shared)
                .write
                .load(Ordering::Relaxed)
                .saturating_sub((*self.shared).read.load(Ordering::Relaxed))
        }
    }

    /// Best-effort `len() == 0`.
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The receiving (consuming) side of the queue.
#[derive(Debug)]
pub struct Receiver<T, const N: usize> {
    shared: *mut Shared<T, N>,
}
unsafe impl<T, const N: usize> Send for Receiver<T, N> where T: Send {}
unsafe impl<T, const N: usize> Sync for Receiver<T, N> where T: Send {}

impl<T, const N: usize> Receiver<T, N> {
    /// Wraps an already-initialised shared-memory region (see
    /// [`spsc_in_memory`]). `new` does not re-initialise the memory.
    ///
    /// # Safety
    /// `shared` must be a valid, initialised `Shared<T, N>` region
    /// (see [`spsc_in_memory`]).
    pub const unsafe fn new(shared: *mut Shared<T, N>) -> Self {
        Receiver { shared }
    }

    /// Retrieves the next available element without blocking.
    ///
    /// Returns [`TryRecvError::Empty`] if the queue is currently empty.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let read = unsafe { (*self.shared).read.load(Ordering::Acquire) };
        let rpos = read & (N - 1);
        let slot = unsafe { (*self.shared).mem.get_unchecked(rpos) };
        // Acquire: synchronises with the producer's Release store of
        // `occupied` that enqueued this slot.
        if !slot.occupied.load(Ordering::Acquire) {
            return Err(TryRecvError::Empty);
        }
        let value = unsafe { slot.value.get().replace(None) }.ok_or(TryRecvError::Empty)?;
        // Release: tells the producer this slot is free again (wrap-around).
        slot.occupied.store(false, Ordering::Release);
        // Release: publishes that we consumed this slot.
        unsafe {
            (*self.shared)
                .read
                .store(read.wrapping_add(1), Ordering::Release)
        };
        Ok(value)
    }

    /// Peeks the next element without removing it.
    ///
    /// # Safety
    /// The returned reference is only valid until the next call to
    /// `try_recv`/`try_send` that touches the same slot.
    #[allow(unused)]
    pub fn peek(&self) -> Option<&T> {
        let read = unsafe { (*self.shared).read.load(Ordering::Acquire) };
        let rpos = read & (N - 1);
        let slot = unsafe { (*self.shared).mem.get_unchecked(rpos) };
        if !slot.occupied.load(Ordering::Acquire) {
            return None;
        }
        unsafe { (*slot.value.get()).as_ref() }
    }

    /// The queue's total capacity in items.
    #[allow(unused)]
    pub const fn capacity(&self) -> usize {
        N
    }

    /// Best-effort number of items currently queued (relaxed atomic read).
    #[allow(unused)]
    pub fn len(&self) -> usize {
        unsafe {
            (*self.shared)
                .write
                .load(Ordering::Relaxed)
                .saturating_sub((*self.shared).read.load(Ordering::Relaxed))
        }
    }

    /// Best-effort `len() == 0`.
    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Returned from [`Sender::try_send`] when the queue is full.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendError<T> {
    /// The queue is full; the consumer must dequeue before sending more.
    NoSpaceLeft(T),
}

impl<T: Debug> Error for SendError<T> {}
impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::NoSpaceLeft(_) => write!(f, "No space left in the SPSC queue."),
        }
    }
}

/// Returned from [`Receiver::try_recv`] when the queue is empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// The queue is currently empty; the producer may still send.
    Empty,
}

impl Error for TryRecvError {}
impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "No data available in the SPSC queue."),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::alloc::{Layout, alloc};
    use std::thread;

    /// Allocates a correctly-aligned `Shared<T, N>` region and creates a
    /// sender/receiver pair over it.
    ///
    /// The backing allocation is intentionally leaked for the lifetime of the
    /// process (tests only ever run once per type/capability and never free
    /// memory while the raw pointers are in use). This mirrors how a real
    /// shared-memory region (mmap'd memfd) would outlive the views.
    fn queue<T, const N: usize>() -> (Sender<T, N>, Receiver<T, N>) {
        let layout = Layout::from_size_align(required_size::<T, N>(), required_align::<T, N>())
            .expect("valid layout");
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "allocation failed");
        unsafe { spsc_in_memory(ptr) }
    }

    #[test]
    fn smoke() {
        let (mut w, mut r) = queue::<u64, 8>();
        assert!(w.is_empty());
        w.try_send(234).unwrap();
        assert_eq!(w.len(), 1);
        assert!(!r.is_empty());
        assert_eq!(r.try_recv(), Ok(234));
        assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn full_empty() {
        let (mut w, mut r) = queue::<usize, 4>();
        for i in 1..=4 {
            assert_eq!(w.try_send(i), Ok(()));
            assert_eq!(w.len(), i);
        }
        assert_eq!(w.try_send(5), Err(SendError::NoSpaceLeft(5)));

        for expect in 1..=4 {
            assert_eq!(r.try_recv(), Ok(expect));
            assert_eq!(w.len(), 4 - expect);
        }
        assert_eq!(r.try_recv(), Err(TryRecvError::Empty));

        // After draining we can send again.
        assert_eq!(w.try_send(99), Ok(()));
        assert_eq!(r.try_recv(), Ok(99));
    }

    #[test]
    fn respects_capacity_limit() {
        let (mut w, mut r) = queue::<usize, 2>();
        // capacity == 2 only; a 3rd send must fail.
        assert_eq!(w.try_send(1), Ok(()));
        assert_eq!(w.try_send(2), Ok(()));
        assert_eq!(w.try_send(3), Err(SendError::NoSpaceLeft(3)));
        assert_eq!(r.try_recv(), Ok(1));
        assert_eq!(r.try_recv(), Ok(2));
        assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(w.capacity(), 2);
        assert_eq!(r.capacity(), 2);
    }

    #[test]
    fn ordering_across_wraparound() {
        // Send 1000 values; the receiver must see them all, exactly once, in
        // the order they were sent. Capacity 8 forces the ring to wrap many
        // times, exercising the free-slot/occupied interaction.
        let (mut w, mut r) = queue::<u32, 8>();

        let producer = thread::Builder::new()
            .name("spsc-producer".into())
            .spawn(move || {
                for i in 0..1_000u32 {
                    loop {
                        match w.try_send(i) {
                            Ok(()) => break,
                            Err(SendError::NoSpaceLeft(v)) => {
                                assert_eq!(v, i);
                                thread::yield_now();
                            }
                        }
                    }
                }
            })
            .expect("spawn producer");

        let mut seen: Vec<u32> = Vec::with_capacity(1_000);
        loop {
            match r.try_recv() {
                Ok(v) => seen.push(v),
                Err(TryRecvError::Empty) => thread::yield_now(),
            }
            if seen.len() >= 1_000 {
                break;
            }
        }
        producer.join().unwrap();

        assert_eq!(seen.len(), 1_000);
        for (i, &v) in seen.iter().enumerate() {
            assert_eq!(v, i as u32, "message #{i} out of order");
        }
    }

    #[test]
    fn peek_then_recv() {
        let (mut w, mut r) = queue::<[u8; 4], 8>();
        assert!(r.peek().is_none());
        w.try_send([1, 2, 3, 4]).unwrap();
        assert_eq!(r.peek(), Some(&[1, 2, 3, 4]));
        // peek does not consume: receiving again returns the same value.
        assert_eq!(r.try_recv(), Ok([1, 2, 3, 4]));
        assert!(r.peek().is_none());
    }
}
