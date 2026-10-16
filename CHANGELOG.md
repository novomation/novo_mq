# Changelog

All notable changes to this project will be documented in this file.

## [unreleased]

### Fixed

* Treat synchronous completion of a non-blocking connect on Windows as a successful connection instead of an error, so returning subscribers can reconnect to a running publisher.
* Stop `UnixDatagram::drop` from removing `local_addr`: an accepted socket reports the listener's path, so pruning a dead subscriber connection deleted the publisher's listener file and no returning subscriber could connect again (Linux and Windows).
* Make domain directory cleanup robust for shared domains: dropping a datalayer handle or a publisher only ever removes the domain directory while it is empty, so a domain with a running publisher is never deleted out from under it; and `Publisher::new` recreates the domain directory if a concurrent cleanup removed it, so no publisher creation can fail because of it.
* Fix a deadlock in `Publisher::new`: the management-thread init handshake shared the subscriber-count condvar, so an accept landing before the creating thread reached the init wait blocked publisher creation forever (observed as an indefinite hang under load with a subscriber that connected early). The subscriber count now starts at zero, the true initial state, and the init handshake is removed.

### Added

* Regression tests for returning-subscriber reconnection, for shared-domain cleanup (domain survives handle drops, is cleaned up when empty, and a publisher recreates a removed domain), and for a subscriber connecting before the publisher is ready.

## [0.5.0] - 2026-07-07

### Added

* Detect active vs stale socket endpoints when creating publishers on Unix and Windows.
* Reuse stale socket paths by removing dangling socket files while still rejecting actively used addresses.

### Changed

* 🚨 Normalize publisher bind failures to report address-in-use semantics when a topic socket is already active.
* Update dependencies

### Fixed

* Improve reconnect coverage for returning subscribers with additional end-to-end test updates.

## [0.4.0] - 2026-06-03

### Added

* Add Windows support by implementing the Windows AF_UNIX socket API
* Add CHANGELOG

### Changed

* 🚨 Extend error types to enable multi-platform support (`SendError`, `TransportError`)
* Refactor the internal transport API to follow a similar approach to [`std::os::unix::net`](https://doc.rust-lang.org/std/os/unix/net/index.html)
* Refactor transport tests to use the same test suite across all supported operating systems
* Extend CI to build on all supported operating systems

### Removed

* 🚨 Removed `Datalayer::open_with_custom_affinity`

## [0.3.1] - 2026-05-08

### Changed

* Update dependencies

## [0.3.0] - 2026-04-14

### Added

* Extend crate metadata
* Add a minimum supported Rust version (MSRV) check to CI

### Changed

* 🚨 Rename crate from `rt-datalayer` to `novomq`

## [0.2.0] - 2026-03-19

### Added

* Initial release for sending and receiving messages on Linux
* Add RTSAN checks for RT capabilities
