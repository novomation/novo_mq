# Changelog

All notable changes to this project will be documented in this file.

## [unreleased]

### Changed

* Update dependencies

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
