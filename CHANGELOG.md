# Changelog

All notable changes to this project will be documented in this file.

## [unreleased]

### ✨ Features

* Add Windows support by implementing the Windows AF_UNIX socket API
* 🚨 Extend error types to enable multi-platform support

### ♻️ Refactor

* Refactor the internal transport API to follow a similar approach to [`std::os::unix::net`](https://doc.rust-lang.org/std/os/unix/net/index.html)
* Refactor transport tests to use the same test suite across all supported operating systems

### ⚙️ Miscellaneous Tasks

* Extend CI to build on all supported operating systems
* Add CHANGELOG

## [0.3.1] - 2026-05-08

### ⚙️ Miscellaneous Tasks

* Update dependencies

## [0.3.0] - 2026-04-14

### ♻️ Refactor

* 🚨 Rename crate from `rt-datalayer` to `novomq`

### ⚙️ Miscellaneous Tasks

* Extend crate metadata
* Add a minimum supported Rust version (MSRV) check to CI

## [0.2.0] - 2026-03-19

### ✨ Features

* Initial release for sending and receiving messages on Linux

### ⚙️ Miscellaneous Tasks

* Add RTSAN checks for RT capabilities
