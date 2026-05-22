//! Shared observability substrates.
//!
//! `observability::http` owns bounded HTTP/1.1 request/response helpers.
//! `observability::audit` owns durable audit mechanics.

#![deny(unsafe_code)]

#[cfg(feature = "audit")]
pub mod audit;

#[cfg(feature = "http")]
pub mod http;
