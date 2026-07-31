//! Bounded HTTP/1.1 helpers for observability listeners and
//! inspect-style clients.
//!
//! This crate owns only narrow request/response IO helpers.
//! Routing, TLS, status semantics, timing policy, metrics, audit,
//! and daemon lifecycle remain caller-owned.

#![deny(unsafe_code)]

pub mod request;
pub mod response;
pub mod writer;

pub const MAX_REQUEST_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_RESPONSE_HEADER_BYTES: usize = 8 * 1024;
pub const MAX_RESPONSE_BODY_BYTES: usize = 64 * 1024;

pub use request::{RequestLine, parse_request_line, read_request};
pub use response::{HttpResponse, read_response};
pub use writer::{write_request, write_response};
