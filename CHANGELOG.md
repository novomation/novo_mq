# Changelog

All notable changes to this project will be documented in this file.

## [unreleased]

### Fixed

* Treat synchronous completion of a non-blocking connect on Windows as a successful connection instead of an error, so returning subscribers can reconnect to a running publisher.
* Stop `UnixDatagram::drop` from removing `local_addr`: an accepted socket reports the listener's path, so pruning a dead subscriber connection deleted the publisher's listener file and no returning subscriber could connect again (Linux and Windows).

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
